#!/usr/bin/env python
from argparse import ArgumentParser
import logging
import sys
from tempfile import NamedTemporaryFile

from lxml import etree
import requests
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


parser = ArgumentParser(description='Load a month of National Caseload Data.')
parser.add_argument(
    '--database-url', help='SQLAlchemy database URL', required=True)
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


def load_file_from_url(zip_file_url, engine):
    """Download a data file and load it into a database.

    Args:
        zip_file_url: A string URL to an NCD data file.
        engine: A sqlalchemy.engine.Engine.
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

        logger.debug('Saving {0} to database'.format(zip_file_basename))
        DataZip(zip_file.name, engine).load()
        logger.debug('Completed {0}'.format(zip_file_basename))


def main(raw_args):
    args = parser.parse_args(raw_args)

    engine = create_engine(args.database_url)

    file_urls = get_file_urls(args.file_listing_url)
    logger.info('Found {0} files to download'.format(len(file_urls)))

    for file_url in file_urls:
        load_file_from_url(file_url, engine)


if __name__ == '__main__':
    main(sys.argv[1:])
