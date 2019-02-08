#!/usr/bin/env python
from argparse import ArgumentParser
import logging
import sys

from sqlalchemy import create_engine

from ncd.data_zip import DataZip


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


parser = ArgumentParser(description='Load a file of National Caseload Data.')
parser.add_argument(
    '--database-url', help='SQLAlchemy database URL', required=True)
parser.add_argument('zip_path', help='Path to a zip file from NCD')


def main(raw_args):
    args = parser.parse_args(raw_args)
    engine = create_engine(args.database_url)
    DataZip(args.zip_path, engine).load()


if __name__ == '__main__':
    main(sys.argv[1:])
