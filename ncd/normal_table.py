from csv import DictReader
import datetime
from gzip import GzipFile
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

    FIXME: This doesn't yet take into account that a table may span multiple
    zip files. Partitioning probably is the best way to handle this.

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
        with NamedTemporaryFile('w+b') as raw_file:
            with GzipFile(fileobj=raw_file) as gzip_file:
                text_gzip_file = TextIOWrapper(gzip_file, encoding='utf-8')
                for data_file_name in data_file_names:
                    self._convert_raw_file(data_file_name, text_gzip_file)
            self._athena.upload_data(self._name, raw_file)
        ddl = self._generate_ddl()
        self._athena.execute_query(ddl)
        self.logger.info('Loaded normal table {0}'.format(self._name))

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
                '%Y-%m-%d')

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

    def _generate_ddl(self):
        """Generate a CREATE EXTERNAL TABLE query to run on Athena.

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
        column_specs = ',\n            '.join(columns)

        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {name} (
                {columns}
            )
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            STORED AS TEXTFILE
            LOCATION 's3://{bucket}/{table_prefix}';
        """.format(
            name=self._name, columns=column_specs,
            bucket=self._athena.data_bucket,
            table_prefix=self._athena.prefix_for_table(self._name)
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
            A tuple of string filenames to use within self._zip.
        """
        lowercase_name = self.name.lower()

        def file_is_for_table(file_name):
            if file_name == lowercase_name + '.txt':
                return True
            return file_name.startswith(lowercase_name + '_')
        data_file_names = tuple(
            filter(file_is_for_table, self._zip.namelist()))

        # If we have a file named exactly for this table, then ignore
        # everything else; this is in order to distinguish between, for
        # example, GS_CASE and GS_CASE_CAUSE_ACT.
        if '{0}.txt'.format(lowercase_name) in data_file_names:
            data_file_names = ('{0}.txt'.format(lowercase_name),)
        self.logger.info('Found {0} file names for table {1}: {2}'.format(
            len(data_file_names), self.name, ', '.join(data_file_names)))

        return data_file_names

    def _make_csv(self, fixed_width_data):
        """Convert a fixed-width data file to a CSV.

        Args:
            fixed_width_data: A text file-like object containing fixed-width
                data, following the format described in self._schema.

        Returns:
            A text file-like object containing CSV data.
        """
        fixed_width_data.seek(0)
        fixed_width_text = TextIOWrapper(fixed_width_data, encoding='utf-8')

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
