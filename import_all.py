#!/usr/bin/env python
from argparse import ArgumentParser
import asyncio
import logging
import sys
from tempfile import NamedTemporaryFile

import aiohttp
from lxml import etree
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


async def get_file_urls(file_listing_url, session):
    """Determine which URLs need to be downloaded.

    Args:
        file_listing_url: A string URL to a page on the DOJ site.
        session: An aiohttp.ClientSession to use.

    Returns:
        A tuple of string URLs to individual zip files.
    """
    async with session.get(file_listing_url) as response:
        raw_html = await response.text()
        html = etree.HTML(raw_html)
    links = html.cssselect('a[href$=".zip"]')
    return tuple(map(lambda link: link.attrib['href'], links))


async def load_file_from_url(zip_file_url, engine, session):
    """Download a data file and load it into a database.

    Args:
        zip_file_url: A string URL to an NCD data file.
        engine: A sqlalchemy.engine.Engine.
        session: An aiohttp.ClientSession to use.
    """
    zip_file_basename = zip_file_url.split('/')[-1]
    logger.debug('About to download {0}'.format(zip_file_basename))
    with NamedTemporaryFile() as zip_file:
        chunk_size = 32768
        async with session.get(zip_file_url, timeout=0) as response:
            logger.debug('Saving {0} to {1}'.format(
                zip_file_basename, zip_file.name))
            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    logger.debug('Finished saving {0} to {1}'.format(
                        zip_file_basename, zip_file.name))
                    break
                zip_file.write(chunk)
        zip_file.seek(0)

        logger.debug('Saving {0} to database'.format(zip_file_basename))
        DataZip(zip_file.name, engine).load()
        logger.debug('Completed {0}'.format(zip_file_basename))


async def main(raw_args):
    args = parser.parse_args(raw_args)

    engine = create_engine(args.database_url)

    conn = aiohttp.TCPConnector(limit=1)
    async with aiohttp.ClientSession(connector=conn) as session:
        file_urls = await get_file_urls(args.file_listing_url, session)
        logger.info('Found {0} files to download'.format(len(file_urls)))

        async def load_url(file_url):
            await load_file_from_url(file_url, engine, session)

        await asyncio.gather(*map(load_url, file_urls))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv[1:]))
    loop.run_until_complete(asyncio.sleep(1))
    loop.close()
