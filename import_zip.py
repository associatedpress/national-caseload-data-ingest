#!/usr/bin/env python
from argparse import ArgumentParser
import logging
import sys

from ncd.athena_mock import AthenaMock as Athena
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
    '--data-bucket', help='S3 bucket name for data files', required=True)
parser.add_argument(
    '--results-bucket', help='S3 bucket name for query results', required=True)
parser.add_argument(
    '--s3-prefix', help='Prefix for data file paths on S3', required=True)
parser.add_argument('zip_path', help='Path to a zip file from NCD')


def main(raw_args):
    args = parser.parse_args(raw_args)
    athena = Athena(
        data_bucket=args.data_bucket, results_bucket=args.results_bucket,
        s3_prefix=args.s3_prefix)
    DataZip(args.zip_path, athena).load()


if __name__ == '__main__':
    main(sys.argv[1:])
