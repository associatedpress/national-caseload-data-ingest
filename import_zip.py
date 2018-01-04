#!/usr/bin/env python
import csv
import re
from io import StringIO, TextIOWrapper
from itertools import chain, starmap
import logging
from operator import itemgetter
import sys
import tempfile
import zipfile

import agate
import agatesql  # noqa: F401
from csvkit.convert.fixed import fixed2csv
from sqlalchemy import Column, create_engine, MetaData, Table
import sqlalchemy.types


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-= HANDLING NORMAL TABLE DATA FILES -=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def make_schema_io(raw_field_specs):
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

    writer = csv.DictWriter(output_io, field_names)
    writer.writeheader()
    writer.writerows(rows)

    output_io.seek(0)
    return output_io


def extract_schema(readme_fragment):
    raw_field_specs = re.finditer(
        (
            r'^(?P<field_name>[A-Z][^\s]+)\s+(?:NOT NULL)?\s+' +
            r'(?P<field_type>[A-Z][^\s]+)\s+' +
            r'\((?P<start_column>\d+):(?P<end_column>\d+)\)'),
        readme_fragment, re.MULTILINE)
    schema_io = make_schema_io(raw_field_specs)
    return schema_io


def extract_table_schemas(input_zip):
    with input_zip.open('README.TXT', 'r') as readme_file:
        readme = readme_file.read().decode('utf-8')

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
        schema = extract_schema(readme_fragment)
        schemas[table_name] = schema

    return schemas


def get_field_type(field_type_text):
    field_components = re.match(
        r'(?P<type>[^(]+)(?:\((?P<args>.+)\))?', field_type_text)
    field_type_component = field_components.group('type')

    if field_type_component in ('VARCHAR', 'VARCHAR2'):
        length = int(field_components.group('args'))
        return sqlalchemy.types.String(length)

    if field_type_component == 'NUMBER':
        try:
            number_args = tuple(map(
                int, re.split(r',\s*', field_components.group('args'))))
        except TypeError:
            return sqlalchemy.types.BigInteger

        if len(number_args) == 1:
            length = number_args[0]
            if length < 10:
                return sqlalchemy.types.Integer
            return sqlalchemy.types.BigInteger

        if len(number_args) == 2:
            return sqlalchemy.types.Numeric(*number_args)

        raise NotImplementedError(
            'Unsure how to handle a {0}'.format(field_type_text))

    if field_type_component == 'DATE':
        return sqlalchemy.types.Date

    if field_type_component == 'FLOAT':
        return sqlalchemy.types.Float

    raise NotImplementedError(
        'Unsure how to handle a {0}'.format(field_type_text))


def get_agate_type(field_type_text):
    field_components = re.match(
        r'(?P<type>[^(]+)(?:\((?P<args>.+)\))?', field_type_text)
    field_type_component = field_components.group('type')

    if field_type_component in ('VARCHAR', 'VARCHAR2'):
        return agate.Text()

    if field_type_component == 'NUMBER':
        return agate.Number()

    if field_type_component == 'DATE':
        return agate.Date(date_format='%d-%b-%Y')

    if field_type_component == 'FLOAT':
        return agate.Number()

    raise NotImplementedError(
        'Unsure how to handle a {0}'.format(field_type_text))


def ensure_table_exists(table_name, table_schema, connection):
    logger = logging.getLogger(__name__).getChild('ensure_table_exists')

    metadata = MetaData(connection)
    metadata.reflect()
    if table_name in metadata.tables:
        logger.debug('Table {0} already exists'.format(table_name))
        return metadata.tables[table_name]

    table_schema.seek(0)
    schema_reader = csv.DictReader(table_schema)

    def build_columns(row):
        data_column = Column(row['column'], get_field_type(row['field_type']))
        redaction_column = Column(
            'redacted_{0}'.format(row['column']), sqlalchemy.types.Boolean)
        return (data_column, redaction_column)

    column_pairs = tuple(map(build_columns, schema_reader))
    data_columns = map(itemgetter(0), column_pairs)
    redaction_columns = map(itemgetter(1), column_pairs)
    columns = tuple(chain(data_columns, redaction_columns))

    table = Table(table_name, metadata, *columns)
    metadata.create_all()

    logger.info('Created table {0}'.format(table_name))

    return table


def build_agate_types(table_schema):
    logger = logging.getLogger(__name__).getChild('build_agate_types')

    table_schema.seek(0)
    schema_reader = csv.DictReader(table_schema)

    def build_column(row):
        return (row['column'], get_agate_type(row['field_type']))
    columns = dict(map(build_column, schema_reader))

    logger.info('Converted column types for agate')

    return columns


def load_table(name=None, schema=None, input_zip=None, connection=None):
    logger = logging.getLogger(__name__).getChild('load_table')

    def file_is_for_table(file_name):
        if file_name == name.lower() + '.txt':
            return True
        return file_name.startswith(name.lower() + '_')
    data_file_names = tuple(filter(file_is_for_table, input_zip.namelist()))
    logger.info('Found {0} file names for table {1}: {2}'.format(
        len(data_file_names), name, ', '.join(data_file_names)))

    for data_file_name in data_file_names:
        ensure_table_exists(name, schema, connection)
        logger.debug('Ensured table {0} exists'.format(name))

        schema.seek(0)
        raw_data_file = input_zip.open(data_file_name)
        wrapped_raw_file = TextIOWrapper(raw_data_file, encoding='utf-8')

        data_csv_file = tempfile.TemporaryFile(mode='w+')
        fixed2csv(wrapped_raw_file, schema, output=data_csv_file)
        data_csv_file.seek(0)
        logger.debug('Converted raw data file {0} to temporary CSV'.format(
            data_file_name))
        wrapped_raw_file.close()
        raw_data_file.close()

        with_redactions_file = tempfile.TemporaryFile(mode='w+')
        with_redactions_writer = csv.writer(with_redactions_file)
        data_csv_reader = csv.reader(data_csv_file)
        header_written = False
        for raw_row in data_csv_reader:
            if header_written:
                data_values = [
                    (item if item != '*' else '') for item in raw_row]
                redaction_values = [
                    ('1' if item == '*' else '0') for item in raw_row]
            else:
                data_values = raw_row
                redaction_values = [
                    'redacted_{0}'.format(item) for item in raw_row]
                header_written = True
            output_row = data_values + redaction_values
            with_redactions_writer.writerow(output_row)
        logger.debug('Separated redactions from data values')
        data_csv_file.close()

        with_redactions_file.seek(0)
        agate_types = build_agate_types(schema)
        csv_table = agate.Table.from_csv(
            with_redactions_file, sniff_limit=0, column_types=agate_types)
        logger.debug('Loaded CSV into agate table')
        csv_table.to_sql(
            connection, name, overwrite=False, create=False,
            create_if_not_exists=False, insert=True)
        logger.info('Done loading data file {0} into table {1}'.format(
            data_file_name, name))
        with_redactions_file.close()


def import_tables_with_schemas(table_schemas, input_zip, connection):
    logger = logging.getLogger(__name__).getChild('import_tables_with_schemas')

    logger.info('Found {0} table schemas: {1}'.format(
        len(table_schemas.keys()),
        ', '.join(sorted(table_schemas.keys()))))

    table_names = sorted(table_schemas.keys())
    for table_name in table_names:
        table_schema = table_schemas[table_name]
        load_table(
            name=table_name, schema=table_schema, input_zip=input_zip,
            connection=connection)
        logger.info('Loaded table {0}'.format(table_name))


# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-= HANDLING LOOKUPS IN global_LIONS.txt -=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def convert_camel_case_field_name(field_name):
    def add_underscore(match):
        return '_' + match.group(1)
    converted = re.sub(r'(?<!^)([A-Z])', add_underscore, field_name).upper()
    if converted.startswith('REDACTED__'):
        converted = converted.replace('REDACTED__', 'redacted_', 1)
    return converted


def extract_global_table(raw_text):
    header, divider, *fixed_rows = raw_text.split('\n')
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
            data_cells = [(cell if cell != '*' else '') for cell in raw_cells]
            redaction_cells = [
                ('t' if cell == '*' else '') for cell in raw_cells]

        return data_cells + redaction_cells

    field_names = split_row(header, True)
    field_names = tuple(map(convert_camel_case_field_name, field_names))
    rows = tuple(map(split_row, fixed_rows))

    table_io = StringIO()
    writer = csv.writer(table_io)
    writer.writerow(field_names)
    writer.writerows(rows)

    table_io.seek(0)
    return table_io


def extract_global_tables(raw_text):
    tables = {}

    table_names = re.findall(r'^([A-Z][^\s]+)$', raw_text, re.MULTILINE)
    if not table_names:
        return tables

    def get_table_start(table_name):
        start_match = re.search(
            r'(?<=^' + table_name + r'\n\n)', raw_text, re.MULTILINE)
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
        global_fragment = raw_text[table_start:table_end]
        next_name_match = re.search(
            r'\n*^[A-Z][^\s]+$\s*', global_fragment, re.MULTILINE)
        if next_name_match:
            global_fragment = global_fragment[:next_name_match.start()]
        global_fragment = global_fragment.strip()
        schema = extract_global_table(global_fragment)
        tables[table_name] = schema

    return tables


def import_global_table(table_name, table_io, connection):
    logger = logging.getLogger(__name__).getChild('import_global_table')

    only_strings = agate.TypeTester(limit=1, types=(agate.Text(),))

    csv_table = agate.Table.from_csv(
        table_io, sniff_limit=0, column_types=only_strings)
    logger.debug('Loaded CSV into agate table')

    csv_table.to_sql(
        connection, table_name, overwrite=True, create=True,
        create_if_not_exists=True, insert=True)
    logger.info('Done loading data into table {0}'.format(table_name))


def load_global_file(input_zip, connection):
    logger = logging.getLogger(__name__).getChild('load_global_file')

    try:
        global_file_info = input_zip.getinfo('global_LIONS.txt')
    except KeyError:
        logger.info('No global file detected')
        return

    with input_zip.open(global_file_info, 'r') as input_file:
        raw_global_text = input_file.read().decode('utf-8')

    tables = extract_global_tables(raw_global_text)
    table_names = sorted(tables.keys())
    logger.debug('Parsed {0} tables in global file: {1}'.format(
        len(table_names), ', '.join(table_names)))

    for table_name in table_names:
        import_global_table(table_name, tables[table_name], connection)


# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=- HANDLING LOOKUPS IN INDIVIDUAL TABLE FILES =-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def load_lookup_tables(input_zip, connection):
    logger = logging.getLogger(__name__).getChild('load_lookup_tables')

    lookup_table_file_names = sorted(tuple(filter(
        lambda name: name.startswith('table_gs_'), input_zip.namelist())))
    logger.info('Found {0} files with lookup tables: {1}'.format(
        len(lookup_table_file_names), ', '.join(lookup_table_file_names)))

    for file_name in lookup_table_file_names:
        with input_zip.open(file_name, 'r') as input_file:
            raw_text = input_file.read().decode('utf-8')

        table_name = re.search(r'(?<=\s)GS_[^\s]+', raw_text).group(0)
        blank_line_matches = tuple(
            re.finditer(r'^[\s\n]*$', raw_text, re.MULTILINE))
        table_start = blank_line_matches[0].end()
        table_end = blank_line_matches[1].start()

        raw_table = raw_text[table_start:table_end].strip()
        logger.debug('Isolated lookup table {0}'.format(table_name))

        table_io = extract_global_table(raw_table)
        logger.debug('Extracted lookup table {0}'.format(table_name))

        import_global_table(table_name, table_io, connection)


# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-= TYING EVERYTHING TOGETHER =-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def main(input_path, database_url):
    logger = logging.getLogger(__name__).getChild('main')

    engine = create_engine(database_url)
    connection = engine.connect()
    logger.info('Connected to database at {0}'.format(database_url))

    with zipfile.ZipFile(input_path, 'r') as input_zip:
        logger.info('Opened input file {0}'.format(input_path))

        table_schemas = extract_table_schemas(input_zip)
        import_tables_with_schemas(table_schemas, input_zip, connection)

        load_global_file(input_zip, connection)
        load_lookup_tables(input_zip, connection)

    connection.close()
    logger.info('Done')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write(
            'Usage: {0} input_path database_url\n'.format(sys.argv[0]))
        sys.exit(1)

    main(*sys.argv[1:])
