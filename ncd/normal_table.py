from csv import DictReader
import datetime
from io import TextIOWrapper
from itertools import chain
import logging
from operator import itemgetter
import re
from tempfile import TemporaryFile

from csvkit.convert.fixed import fixed2csv
from sqlalchemy import (
    BigInteger, Boolean, Column, Date, Float, MetaData, String, Table)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class NormalTable(object):
    """Helper to import raw fixed-width data to Athena.

    Args:
        name: A string name for the NCD table being imported.
        zip_file: A zipfile.ZipFile of NCD data.
        schema_io: A text file-like object with field information for the
            table's raw data.
        engine: A sqlalchemy.engine.Engine.
    """

    def __init__(self, name=None, zip_file=None, schema_io=None, engine=None):
        self.name = name
        self._zip = zip_file
        self._schema = schema_io
        self._engine = engine
        self._metadata = MetaData()
        self.logger = logger.getChild('NormalTable')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def load(self):
        """Load this table's data into Athena."""
        data_file_names = self._get_file_names()
        districts = sorted(data_file_names.keys())

        is_partitioned = None not in districts

        db_table = self._create_table(is_partitioned)
        self.logger.debug('Ensured table exists for {0}'.format(self.name))

        for district in districts:
            district_file_name = data_file_names[district]
            self._insert_from_raw_file(district_file_name, db_table)

        self.logger.info('Loaded normal table {0}'.format(self.name))

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _create_table(self, is_partitioned=False):
        """Create a database table, deleting an existing one if necessary.

        Args:
            is_partitioned: A boolean specifying whether a table is to be split
                into multiple files by federal judicial district (True) or
                consists of only one file covering all districts (False).

        Returns:
            A sqlalchemy.table.Table.
        """
        self._schema.seek(0)
        reader = DictReader(self._schema)

        def get_sqlalchemy_type(field_type_text):
            field_components = re.match(
                r'(?P<type>[^(]+)(?:\((?P<args>.+)\))?', field_type_text)
            field_type_component = field_components.group('type')
            if field_type_component in ('VARCHAR', 'VARCHAR2'):
                return String
            if field_type_component == 'NUMBER':
                return BigInteger
            if field_type_component == 'DATE':
                return Date
            if field_type_component == 'FLOAT':
                return Float
            raise NotImplementedError(
                'Unsure how to handle a {0}'.format(field_type_text))

        def build_column(row):
            data_column = Column(
                row['column'], get_sqlalchemy_type(row['field_type']))
            redaction_column = Column(
                'redacted_{0}'.format(row['column']), Boolean)
            return (data_column, redaction_column)

        column_pairs = tuple(map(build_column, reader))
        data_columns = map(itemgetter(0), column_pairs)
        redaction_columns = map(itemgetter(1), column_pairs)
        columns = tuple(chain(data_columns, redaction_columns))

        table = Table(self.name, self._metadata, *columns)

        conn = self._engine.connect()
        if not is_partitioned:
            table.drop(conn, checkfirst=True)
        table.create(conn, checkfirst=True)
        conn.close()

        return table

    def _gather_python_types(self):
        """Determine which Python data type each field should have.

        Returns:
            A dict with field names as keys and functions as values.
        """
        self._schema.seek(0)
        schema_reader = DictReader(self._schema)

        def _parse_oracle_date(raw_text):
            return datetime.datetime.strptime(raw_text, '%d-%b-%Y').strftime(
                '%Y-%m-%d').rjust(10, '0')

        def converter_with_nulls(converter):
            def convert(raw_text):
                try:
                    return converter(raw_text)
                except ValueError:
                    return None
            return convert

        def get_python_type(field_type_text):
            field_components = re.match(
                r'(?P<type>[^(]+)(?:\((?P<args>.+)\))?', field_type_text)
            field_type_component = field_components.group('type')
            if field_type_component in ('VARCHAR', 'VARCHAR2'):
                return converter_with_nulls(str)
            if field_type_component == 'NUMBER':
                return converter_with_nulls(int)
            if field_type_component == 'DATE':
                return converter_with_nulls(_parse_oracle_date)
            if field_type_component == 'FLOAT':
                return converter_with_nulls(float)
            raise NotImplementedError(
                'Unsure how to handle a {0}'.format(field_type_text))

        def build_column(row):
            return (row['column'], get_python_type(row['field_type']))

        return dict(map(build_column, schema_reader))

    def _generate_rows(self, csv_data, batch_size=100000):
        """Convert rows of a CSV.

        Args:
            csv_data: A text file-like object containing CSV data.
            batch_size: An int maximum number of rows to yield at once.

        Yields:
            A list of dicts.
        """
        field_converters = self._gather_python_types()
        reader = DictReader(csv_data)

        batch_so_far = 0
        output_objs = []
        for input_row in reader:
            output_obj = {}
            for field_name, field_raw_value in input_row.items():
                if field_raw_value == '*':
                    field_value = None
                    redacted_value = True
                else:
                    field_value = field_converters[field_name](field_raw_value)
                    redacted_value = False
                output_obj[field_name] = field_value
                output_obj['redacted_{0}'.format(field_name)] = redacted_value

            output_objs.append(output_obj)
            batch_so_far += 1

            if batch_so_far >= batch_size:
                yield output_objs
                output_objs.clear()
                batch_so_far = 0

        yield output_objs

    def _get_file_names(self):
        """Determine which contents to use from our zip file.

        Returns:
            A dict. Each key specifies the federal judicial district covered by
            a given data file; this is a string unless the file covers all
            districts, which case it is None. Each value is a string filename
            for the given data file within self._zip.
        """
        lowercase_name = self.name.lower()
        file_name_pattern = re.compile(''.join([
            r'^', lowercase_name, r'(?:_(?P<district>[A-Z]+))?\.txt$']))

        def file_is_for_table(file_name):
            match = file_name_pattern.match(file_name)
            if not match:
                return None
            return (match.group('district'), file_name)
        data_file_names = dict(
            filter(None, map(file_is_for_table, self._zip.namelist())))

        return data_file_names

    def _insert_from_raw_file(self, raw_path, db_table):
        """Convert a raw data file for Athena and add it to a .gz.

        Args:
            raw_path: A string path to a file stored in self._zip.
            db_table: A sqlalchemy.table.Table.
        """
        self.logger.debug('Beginning conversion of {0}'.format(raw_path))

        with self._zip.open(raw_path) as raw_data:
            without_carriage_returns = self._remove_crs(raw_data)
        csv_data = self._make_csv(without_carriage_returns)

        for rows_to_insert in self._generate_rows(csv_data):
            conn = self._engine.connect()
            conn.execute(db_table.insert(), rows_to_insert)
            conn.close()
            self.logger.debug('Inserted {0} rows of {1}'.format(
                len(rows_to_insert), self.name))

        self.logger.debug('Completed conversion of {0}'.format(raw_path))

    def _make_csv(self, fixed_width_data):
        """Convert a fixed-width data file to a CSV.

        Args:
            fixed_width_data: A text file-like object containing fixed-width
                data, following the format described in self._schema.

        Returns:
            A text file-like object containing CSV data.
        """
        self._schema.seek(0)
        fixed_width_data.seek(0)
        fixed_width_text = TextIOWrapper(fixed_width_data, encoding='latin-1')

        csv_file = TemporaryFile(mode='w+')
        fixed2csv(fixed_width_text, self._schema, output=csv_file)

        fixed_width_text.close()
        csv_file.seek(0)

        self.logger.debug('Converted fixed-width data to CSV')
        return csv_file

    def _remove_crs(self, raw_data):
        """Remove carriage returns from a file.

        Args:
            raw_data: A file-like object.

        Returns:
            A file-like object with most of the same content.
        """
        no_cr_file = TemporaryFile(mode='w+b')
        while True:
            raw_chunk = raw_data.read(4096)
            if not raw_chunk:
                break
            fixed_chunk = raw_chunk.replace(b'\r', b' ')
            no_cr_file.write(fixed_chunk)

        no_cr_file.seek(0)
        raw_data.close()

        self.logger.debug('Removed carriage returns')
        return no_cr_file
