#!/usr/bin/env python
from argparse import ArgumentParser
import logging
import sys
from tempfile import NamedTemporaryFile

from lxml import etree
import requests

from ncd.athena import Athena as Athena
from ncd.data_zip import DataZip


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


parser = ArgumentParser(description='Load a month of National Caseload Data.')
parser.add_argument(
    '--data-bucket', help='S3 bucket name for data files', required=True)
parser.add_argument(
    '--results-bucket', help='S3 bucket name for query results', required=True)
parser.add_argument(
    '--s3-prefix', help='Prefix for data file paths on S3', required=True)
parser.add_argument('--db-name', help='Database name on Athena', required=True)
parser.add_argument(
    'file_listing_url',
    help='URL to a DOJ page of yearly or monthly data files')


def get_file_urls(file_listing_url):
    """Determine which URLs need to be downloaded.

    Args:
        file_listing_url: A string URL to a page on the DOJ site.

    Returns:
        A tuple of string URLs to individual zip files.
    """
    r = requests.get(file_listing_url)
    raw_html = r.text
    html = etree.HTML(raw_html)
    links = html.cssselect('a[href$=".zip"]')
    return tuple(map(lambda link: link.attrib['href'], links))


def load_file_from_url(zip_file_url, athena):
    """Download a data file and load it into a database.

    Args:
        zip_file_url: A string URL to an NCD data file.
        athena: An ncd.Athena to use when storing the file.
    """
    zip_file_basename = zip_file_url.split('/')[-1]
    logger.debug('About to download {0}'.format(zip_file_basename))
    with NamedTemporaryFile() as zip_file:
        chunk_size = 32768
        r = requests.get(zip_file_url, stream=True)
        logger.debug('Saving {0} to {1}'.format(
            zip_file_basename, zip_file.name))
        for chunk in r.iter_content(chunk_size=chunk_size):
            zip_file.write(chunk)
        logger.debug('Finished saving {0} to {1}'.format(
            zip_file_basename, zip_file.name))
        zip_file.seek(0)

        logger.debug('Saving {0} to Athena'.format(zip_file_basename))
        DataZip(zip_file.name, athena).load()
        logger.debug('Completed {0}'.format(zip_file_basename))


def main(raw_args):
    args = parser.parse_args(raw_args)

    athena = Athena(
        data_bucket=args.data_bucket, results_bucket=args.results_bucket,
        s3_prefix=args.s3_prefix, db_name=args.db_name)
    athena.create_db()

    file_urls = get_file_urls(args.file_listing_url)
    logger.info('Found {0} files to download'.format(len(file_urls)))

    for file_url in file_urls:
        load_file_from_url(file_url, athena)


if __name__ == '__main__':
    main(sys.argv[1:])
