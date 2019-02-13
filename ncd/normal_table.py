from csv import DictReader
import datetime
import gzip
from io import TextIOWrapper
from itertools import chain
import json
import logging
from operator import itemgetter
import re
from tempfile import NamedTemporaryFile, TemporaryFile
from textwrap import dedent

from csvkit.convert.fixed import fixed2csv


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
        athena: An ncd.Athena to use when accessing AWS.
    """

    def __init__(self, name=None, zip_file=None, schema_io=None, athena=None):
        self.name = name
        self._zip = zip_file
        self._schema = schema_io
        self._athena = athena
        self.logger = logger.getChild('NormalTable')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def load(self):
        """Load this table's data into Athena."""
        data_file_names = self._get_file_names()
        districts = sorted(data_file_names.keys())
        for district in districts:
            district_file_name = data_file_names[district]
            with NamedTemporaryFile('w+b') as raw_file:
                with gzip.open(raw_file, 'wb') as gzip_file:
                    text_gzip_file = TextIOWrapper(gzip_file, encoding='utf-8')
                    self._convert_raw_file(district_file_name, text_gzip_file)
                    text_gzip_file.close()
                self._athena.upload_data(
                    self.name, raw_file, district=district)

        is_partitioned = None not in districts

        ddl = self._generate_ddl(is_partitioned)
        self._athena.execute_query(ddl)
        self.logger.debug('Ensured table exists for {0}'.format(self.name))

        if is_partitioned:
            self._athena.execute_query(
                'MSCK REPAIR TABLE {0};'.format(self.name))
            self.logger.debug('Repaired table for {0}'.format(self.name))

        self.logger.info('Loaded normal table {0}'.format(self.name))

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _convert_raw_file(self, raw_path, gzip_file):
        """Convert a raw data file for Athena and add it to a .gz.

        Args:
            raw_path: A string path to a file stored in self._zip.
            gzip_file: A file-like object to which our newly converted data
                should be appended.
        """
        self.logger.debug('Beginning conversion of {0}'.format(raw_path))

        with self._zip.open(raw_path) as raw_data:
            without_carriage_returns = self._remove_crs(raw_data)
        csv_data = self._make_csv(without_carriage_returns)
        self._generate_rows(csv_data, gzip_file)

        self.logger.debug('Completed conversion of {0}'.format(raw_path))

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

    def _generate_ddl(self, is_partitioned=False):
        """Generate a CREATE EXTERNAL TABLE query to run on Athena.

        Args:
            is_partitioned: A boolean specifying whether a table is to be split
                into multiple files by federal judicial district (True) or
                consists of only one file covering all districts (False).

        Returns:
            A string SQL query to execute.
        """
        self._schema.seek(0)
        reader = DictReader(self._schema)

        def get_athena_type(field_type_text):
            field_components = re.match(
                r'(?P<type>[^(]+)(?:\((?P<args>.+)\))?', field_type_text)
            field_type_component = field_components.group('type')
            if field_type_component in ('VARCHAR', 'VARCHAR2'):
                return 'STRING'
            if field_type_component == 'NUMBER':
                return 'BIGINT'
            if field_type_component == 'DATE':
                return 'DATE'  # Actually a date in strftime format '%d-%b-%Y'
            if field_type_component == 'FLOAT':
                return 'DOUBLE'
            raise NotImplementedError(
                'Unsure how to handle a {0}'.format(field_type_text))

        def build_column(row):
            data_column = '{0} {1}'.format(
                row['column'], get_athena_type(row['field_type']))
            redaction_column = 'redacted_{0} BOOLEAN'.format(row['column'])
            return (data_column, redaction_column)

        column_pairs = tuple(map(build_column, reader))
        data_columns = map(itemgetter(0), column_pairs)
        redaction_columns = map(itemgetter(1), column_pairs)
        columns = tuple(chain(data_columns, redaction_columns))
        column_specs = ',\n                '.join(columns)

        if is_partitioned:
            partition_clause = (
                '\n            PARTITIONED BY (filename_district STRING)')
        else:
            partition_clause = ''

        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {name} (
                {columns}
            ){partition_clause}
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            STORED AS TEXTFILE
            LOCATION 's3://{bucket}/{table_prefix}';
        """.format(
            name=self.name, columns=column_specs,
            partition_clause=partition_clause,
            bucket=self._athena.data_bucket,
            table_prefix=self._athena.prefix_for_table(self.name)
        )

        return dedent(query)

    def _generate_rows(self, csv_data, gzip_file):
        """Convert rows of a CSV and append the results to a .gz.

        Args:
            csv_data: A text file-like object containing CSV data.
            gzip_file: A file-like object to which our newly converted data
                should be appended.
        """
        field_converters = self._gather_python_types()
        reader = DictReader(csv_data)
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
            output_json = json.dumps(output_obj)
            gzip_file.write('{0}\n'.format(output_json))

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
