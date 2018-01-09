#!/usr/bin/env python
import csv
import datetime
import gzip
from io import StringIO, TextIOWrapper
from itertools import chain, starmap
import json
import logging
from operator import itemgetter
from os import makedirs
import os.path
import re
from shutil import copyfileobj
import sys
import tempfile
from textwrap import dedent
import zipfile

from csvkit.convert.fixed import fixed2csv


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


def gather_python_types(table_schema):
    table_schema.seek(0)
    schema_reader = csv.DictReader(table_schema)

    def build_column(row):
        return (row['column'], get_python_type(row['field_type']))

    return dict(map(build_column, schema_reader))


def generate_ddl(name, table_schema):
    table_schema.seek(0)
    schema_reader = csv.DictReader(table_schema)

    def build_column(row):
        data_column = '{0} {1}'.format(
            row['column'], get_athena_type(row['field_type']))
        redaction_column = 'redacted_{0} BOOLEAN'.format(row['column'])
        return (data_column, redaction_column)

    column_pairs = tuple(map(build_column, schema_reader))
    data_columns = map(itemgetter(0), column_pairs)
    redaction_columns = map(itemgetter(1), column_pairs)
    columns = tuple(chain(data_columns, redaction_columns))
    column_specs = ',\n            '.join(columns)

    query = """
        CREATE EXTERNAL TABLE {name} (
            {columns}
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION 's3://associatedpress-datateam-athena-data/ncd/test-monthly/{name}';
    """.format(name=name, columns=column_specs)  # noqa: E501

    return dedent(query)


def remove_cr_from_file(input_file):
    no_cr_file = tempfile.TemporaryFile(mode='w+b')
    while True:
        raw_chunk = input_file.read(4096)
        if not raw_chunk:
            break
        fixed_chunk = raw_chunk.replace(b'\r', b' ')
        no_cr_file.write(fixed_chunk)
    no_cr_file.seek(0)
    return no_cr_file


def separate_redactions(input_file, schema):
    logger = logging.getLogger(__name__).getChild('separate_redactions')

    field_converters = gather_python_types(schema)

    output_file = tempfile.TemporaryFile(mode='w+')
    input_reader = csv.DictReader(input_file)

    for input_row in input_reader:
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
        output_file.write(json.dumps(output_obj))
        output_file.write('\n')

    logger.debug('Separated redactions from data values; generated JSON')

    return output_file


def save_gzip_file(file_obj, dest_path):
    file_obj.seek(0)
    makedirs(os.path.dirname(dest_path), exist_ok=True)
    with gzip.open(dest_path, 'wb') as output_file:
        output_text_file = TextIOWrapper(output_file, encoding='utf-8')
        copyfileobj(file_obj, output_text_file)


def load_table(name=None, schema=None, input_zip=None):
    logger = logging.getLogger(__name__).getChild('load_table')

    def file_is_for_table(file_name):
        if file_name == name.lower() + '.txt':
            return True
        return file_name.startswith(name.lower() + '_')
    data_file_names = tuple(filter(file_is_for_table, input_zip.namelist()))
    # If we have a file named exactly for this table, then ignore everything
    # else; this is in order to distinguish between, for example, GS_CASE and
    # GS_CASE_CAUSE_ACT.
    if '{0}.txt'.format(name.lower()) in data_file_names:
        data_file_names = ('{0}.txt'.format(name.lower()),)
    logger.info('Found {0} file names for table {1}: {2}'.format(
        len(data_file_names), name, ', '.join(data_file_names)))

    for data_file_name in data_file_names:
        schema.seek(0)
        raw_data_file = input_zip.open(data_file_name)

        # Replace extraneous carriage returns, and ensure the correct encoding.
        no_cr_file = remove_cr_from_file(raw_data_file)
        raw_data_file.close()
        wrapped_raw_file = TextIOWrapper(no_cr_file, encoding='utf-8')

        # Convert from fixed-width to a CSV, using csvkit.
        data_csv_file = tempfile.TemporaryFile(mode='w+')
        fixed2csv(wrapped_raw_file, schema, output=data_csv_file)
        data_csv_file.seek(0)
        logger.debug('Converted raw data file {0} to temporary CSV'.format(
            data_file_name))
        wrapped_raw_file.close()

        # Add columns to record when values have been redacted.
        with_redactions_file = separate_redactions(data_csv_file, schema)
        data_csv_file.close()

        # TODO: Partition data.
        # TODO: Compress data.

        # Copy the finished CSV to the destination. Currently this is on disk,
        # but eventually we'll make it go to S3 instead.
        output_dir = os.path.join('tables', name)
        output_path = os.path.join(output_dir, '{0}.json.gz'.format(name))
        save_gzip_file(with_redactions_file, output_path)
        logger.debug('Saved {0} to {1}'.format(name, output_path))
        with_redactions_file.close()

        # Generate a DDL query for this table. Currently we save this to disk,
        # but eventually we'll run it directly on Athena.
        ddl = generate_ddl(name, schema)
        output_path = os.path.join('ddl', '{0}.sql'.format(name))
        makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as output_file:
            output_file.write(ddl)
        logger.debug('Saved {name} DDL to {output_path}'.format(
            name=name, output_path=output_path))


def import_tables_with_schemas(table_schemas, input_zip):
    logger = logging.getLogger(__name__).getChild('import_tables_with_schemas')

    logger.info('Found {0} table schemas: {1}'.format(
        len(table_schemas.keys()),
        ', '.join(sorted(table_schemas.keys()))))

    table_names = sorted(table_schemas.keys())
    for table_name in table_names:
        table_schema = table_schemas[table_name]
        load_table(name=table_name, schema=table_schema, input_zip=input_zip)
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


def generate_global_ddl(name, table_io):
    table_io.seek(0)
    reader = csv.reader(table_io)
    field_names = next(reader)

    def build_column(field_name):
        if field_name.startswith('redacted_'):
            return '{0} BOOLEAN'.format(field_name)
        else:
            return '{0} STRING'.format(field_name)

    columns = tuple(map(build_column, field_names))
    column_specs = ',\n            '.join(columns)

    query = """
        CREATE EXTERNAL TABLE {name} (
            {columns}
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION 's3://associatedpress-datateam-athena-data/ncd/test-monthly/{name}';
    """.format(name=name, columns=column_specs)  # noqa: E501

    return dedent(query)


def global_csv_to_json(table_io):
    table_io.seek(0)
    reader = csv.DictReader(table_io)

    output_io = StringIO()
    for input_row in reader:
        output_row = {}
        for key, value in input_row.items():
            if key.startswith('redacted_'):
                output_row[key] = bool(value)
            else:
                output_row[key] = value
        output_io.write(json.dumps(output_row))
        output_io.write('\n')

    return output_io


def load_global_file(input_zip):
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
        json_file = global_csv_to_json(tables[table_name])
        output_dir = os.path.join('tables', table_name)
        output_path = os.path.join(
            output_dir, '{0}.json.gz'.format(table_name))
        save_gzip_file(json_file, output_path)
        logger.debug('Saved {0} to {1}'.format(table_name, output_path))

        # Generate a DDL query for this table. Currently we save this to disk,
        # but eventually we'll run it directly on Athena.
        ddl = generate_global_ddl(table_name, tables[table_name])
        output_path = os.path.join('ddl', '{0}.sql'.format(table_name))
        makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as output_file:
            output_file.write(ddl)
        logger.debug('Saved {name} DDL to {output_path}'.format(
            name=table_name, output_path=output_path))


# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=- HANDLING LOOKUPS IN INDIVIDUAL TABLE FILES =-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def load_lookup_tables(input_zip):
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

        json_file = global_csv_to_json(table_io)
        output_dir = os.path.join('tables', table_name)
        output_path = os.path.join(
            output_dir, '{0}.json.gz'.format(table_name))
        save_gzip_file(json_file, output_path)
        logger.debug('Saved {0} to {1}'.format(table_name, output_path))

        # Generate a DDL query for this table. Currently we save this to disk,
        # but eventually we'll run it directly on Athena.
        ddl = generate_global_ddl(table_name, table_io)
        output_path = os.path.join('ddl', '{0}.sql'.format(table_name))
        makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as output_file:
            output_file.write(ddl)
        logger.debug('Saved {name} DDL to {output_path}'.format(
            name=table_name, output_path=output_path))


# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-= TYING EVERYTHING TOGETHER =-=-=-=-=-=-=-=-=-=-=-=-=-
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def main(input_path):
    logger = logging.getLogger(__name__).getChild('main')

    with zipfile.ZipFile(input_path, 'r') as input_zip:
        logger.info('Opened input file {0}'.format(input_path))

        table_schemas = extract_table_schemas(input_zip)
        import_tables_with_schemas(table_schemas, input_zip)

        load_global_file(input_zip)
        load_lookup_tables(input_zip)

    logger.info('Done')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write(
            'Usage: {0} input_path\n'.format(sys.argv[0]))
        sys.exit(1)

    main(*sys.argv[1:])
