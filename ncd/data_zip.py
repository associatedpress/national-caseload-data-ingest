from csv import DictWriter
from io import StringIO
from itertools import starmap
import logging
import re
from zipfile import ZipFile

from ncd.global_file import GlobalFile
from ncd.lookup_table import LookupTable
from ncd.normal_table import NormalTable


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class DataZip(object):
    """Load all of the data from an NCD zip file to Athena.

    Args:
        zip_path: A string path to a zip file from NCD.
        engine: A sqlalchemy.engine.Engine.
    """

    def __init__(self, zip_path=None, engine=None):
        self._zip_path = zip_path
        self._engine = engine
        self.logger = logger.getChild('DataZip')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def load(self):
        """Load this file's tables into the database."""
        with ZipFile(self._zip_path, 'r') as zip_file:
            logger.info('Opened input file {0}'.format(self._zip_path))
            self._zip_file = zip_file

            normal_schemas = self._extract_normal_schemas()

            self._process_normal_tables(normal_schemas)
            self._process_global_tables()
            self._process_lookup_tables()

        self.logger.info('Done')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _extract_normal_schema(self, raw_fragment):
        """Extract a normal table's schema from a README fragment.

        Args:
            raw_fragment: A string fragment of the zip file's README.

        Returns:
            A text file-like object with CSV schema information in the format
            expected by csvkit's in2csv utility.
        """
        raw_field_specs = re.finditer(
            (
                r'^(?P<field_name>[A-Z][^\s]+)\s+(?:NOT NULL)?\s+' +
                r'(?P<field_type>[A-Z][^\s]+)\s+' +
                r'\((?P<start_column>\d+):(?P<end_column>\d+)\)'),
            raw_fragment, re.MULTILINE)

        def make_row(row):
            start_column = int(row.group('start_column'))
            end_column = int(row.group('end_column'))
            return {
                'column': row.group('field_name'),
                'start': str(start_column),
                'length': str(end_column - start_column + 1),
                'field_type': row.group('field_type')
            }
        rows = map(make_row, raw_field_specs)

        field_names = ('column', 'start', 'length', 'field_type')
        output_io = StringIO()

        writer = DictWriter(output_io, field_names)
        writer.writeheader()
        writer.writerows(rows)

        output_io.seek(0)
        return output_io

    def _extract_normal_schemas(self):
        """Extract schemas for normal data tables.

        Returns:
            A dict with string table names as keys and text file-like objects
            as values. Each value contains a CSV with schema information in the
            format expected by csvkit's in2csv utility.
        """
        with self._zip_file.open('README.TXT', 'r') as readme_file:
            readme = readme_file.read().decode('latin-1')

        schemas = {}

        table_names = re.findall(r'^([A-Z][^ ]+) - ', readme, re.MULTILINE)
        if not table_names:
            return schemas

        def get_table_start(table_name):
            start_match = re.search(
                r'^' + table_name + ' - ', readme, re.MULTILINE)
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
            readme_fragment = readme[table_start:table_end]
            schema = self._extract_normal_schema(readme_fragment)
            schemas[table_name] = schema

        return schemas

    def _process_global_tables(self):
        """Load this file's global (schemaless) tables into Athena."""
        GlobalFile(self._zip_file, self._engine).load()

    def _process_lookup_tables(self):
        """Load this file's separate lookup tables into Athena."""
        table_file_names = sorted(tuple(filter(
            lambda name: name.startswith('table_gs_'),
            self._zip_file.namelist())))
        for file_name in table_file_names:
            with self._zip_file.open(file_name, 'r') as input_file:
                raw_content = input_file.read().decode('latin-1')
            LookupTable(raw_content, self._engine).load()

    def _process_normal_tables(self, schemas):
        """Load this file's normal tables into Athena.

        Args:
            schemas: A dict as returned by _extract_table_schemas.
        """
        table_names = sorted(schemas.keys())
        self.logger.info('Found {0} table schemas: {1}'.format(
            len(table_names), ', '.join(table_names)))
        for table_name in table_names:
            normal_table = NormalTable(
                name=table_name, zip_file=self._zip_file,
                schema_io=schemas[table_name], engine=self._engine)
            normal_table.load()
