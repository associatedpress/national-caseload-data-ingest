from csv import DictReader, reader, writer
import gzip
from io import StringIO, TextIOWrapper
from itertools import starmap
import json
import logging
import re
from tempfile import NamedTemporaryFile
from textwrap import dedent


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class GlobalFile(object):
    """Helper to import from global_LIONS.txt to Athena.

    Args:
        zip_file: A zipfile.ZipFile of NCD data.
        athena: An ncd.Athena to use when accessing AWS.
    """

    def __init__(self, zip_file=None, athena=None):
        self._zip = zip_file
        self._athena = athena
        self.logger = logger.getChild('GlobalFile')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def load(self):
        """Load all tables from this file into Athena."""
        try:
            raw_content = self._get_raw_content()
        except KeyError:
            return
        tables = self._extract_global_tables(raw_content)
        table_names = sorted(tables.keys())
        for table_name in table_names:
            self._load_table(table_name, tables[table_name])
            self.logger.info('Loaded global table {0}'.format(table_name))

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _convert_raw_file(self, table, output_file):
        """Convert a raw data file for Athena and add it to a .gz.

        Args:
            table: A text file-like object with table data.
            output_file: A text file-like object to which our newly converted
                data should be appended.
        """
        table.seek(0)
        reader = DictReader(table)

        for input_row in reader:
            output_row = {}
            for key, value in input_row.items():
                if key.startswith('redacted_'):
                    output_row[key] = bool(value)
                else:
                    output_row[key] = value
            output_file.write(json.dumps(output_row))
            output_file.write('\n')

    def _extract_global_table(self, raw_fragment):
        """Extract a CSV of data for one table.

        Args:
            raw_fragment: A string containing fixed-width data for one table
                from within global_LIONS.txt.

        Returns:
            A text file-like object containing CSV data from the given table.
        """
        header, divider, *fixed_rows = raw_fragment.split('\n')
        field_width_matches = tuple(re.finditer(r'-+', divider))

        def split_row(row, is_header=False):
            def extract_field(match):
                return row[match.start():match.end()].strip()
            raw_cells = tuple(map(extract_field, field_width_matches))
            if is_header:
                data_cells = list(raw_cells)
                redaction_cells = [
                    'redacted_{0}'.format(cell) for cell in raw_cells]
            else:
                data_cells = [
                    (cell if cell != '*' else '') for cell in raw_cells]
                redaction_cells = [
                    ('t' if cell == '*' else '') for cell in raw_cells]
            return data_cells + redaction_cells

        def convert_camel_case_field_name(field_name):
            def add_underscore(match):
                return '_' + match.group(1)
            converted = re.sub(
                r'(?<!^)([A-Z])', add_underscore, field_name).upper()
            if converted.startswith('REDACTED__'):
                converted = converted.replace('REDACTED__', 'redacted_', 1)
            return converted

        field_names = split_row(header, True)
        field_names = tuple(map(convert_camel_case_field_name, field_names))
        rows = tuple(map(split_row, fixed_rows))

        table_io = StringIO()
        table_writer = writer(table_io)
        table_writer.writerow(field_names)
        table_writer.writerows(rows)

        table_io.seek(0)
        return table_io

    def _extract_global_tables(self, raw_content):
        """Split global_LIONS.txt's content into individual tables.

        Args:
            raw_content: A string with the content of global_LIONS.txt.

        Returns:
            A dict with string table names as keys and text file-like objects
            as values. Each value contains CSV data for the given table.
        """
        tables = {}

        table_names = re.findall(r'^([A-Z][^\s]+)$', raw_content, re.MULTILINE)
        if not table_names:
            return tables

        def get_table_start(table_name):
            start_match = re.search(
                r'(?<=^' + table_name + r'\n\n)', raw_content, re.MULTILINE)
            return (table_name, start_match.start())
        table_starts = tuple(map(get_table_start, table_names))
        last_table_name = table_names[-1]

        def get_table_end(i, table_info):
            table_name, table_start = table_info
            if table_name == last_table_name:
                table_end = None
            else:
                table_end = table_starts[i + 1][1]
            return (table_name, table_start, table_end)
        table_info = tuple(starmap(get_table_end, enumerate(table_starts)))

        for table_name, table_start, table_end in table_info:
            global_fragment = raw_content[table_start:table_end]
            next_name_match = re.search(
                r'\n*^[A-Z][^\s]+$\s*', global_fragment, re.MULTILINE)
            if next_name_match:
                global_fragment = global_fragment[:next_name_match.start()]
            global_fragment = global_fragment.strip()
            schema = self._extract_global_table(global_fragment)
            tables[table_name] = schema

        return tables

    def _generate_ddl(self, name, table):
        """Generate a CREATE EXTERNAL TABLE query to run on Athena.

        Args:
            name: A string name for the table being loaded.
            table: A text file-like object with table data.

        Returns:
            A string SQL query to execute.
        """
        table.seek(0)
        table_reader = reader(table)
        field_names = next(table_reader)

        def build_column(field_name):
            if field_name.startswith('redacted_'):
                return '{0} BOOLEAN'.format(field_name)
            else:
                return '{0} STRING'.format(field_name)

        columns = tuple(map(build_column, field_names))
        column_specs = ',\n                '.join(columns)

        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {name} (
                {columns}
            )
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            STORED AS TEXTFILE
            LOCATION 's3://{bucket}/{table_prefix}';
        """.format(
            name=name, columns=column_specs,
            bucket=self._athena.data_bucket,
            table_prefix=self._athena.prefix_for_table(name)
        )

        return dedent(query)

    def _get_raw_content(self):
        """Load the global file into memory if available.

        Returns:
            A string with the content of the global file.

        Raises:
            KeyError: The given zip file doesn't contain a global file.
        """
        try:
            global_file_info = self._zip.getinfo('global_LIONS.txt')
        except KeyError:
            self.logger.info('No global file detected in zip')
            raise KeyError('No global file detected in zip') from None

        with self._zip.open(global_file_info, 'r') as input_file:
            return input_file.read().decode('utf-8')

    def _load_table(self, name, table):
        """Load a single table from the global file to Athena.

        Args:
            name: A string name for the table being loaded.
            table: A text file-like object with table data.
        """
        with NamedTemporaryFile('w+b') as raw_file:
            with gzip.open(raw_file, 'wb') as gzip_file:
                text_gzip_file = TextIOWrapper(gzip_file, encoding='utf-8')
                self._convert_raw_file(table, text_gzip_file)
            self._athena.upload_data(name, raw_file)
        ddl = self._generate_ddl(name, table)
        self._athena.execute_query(ddl)
