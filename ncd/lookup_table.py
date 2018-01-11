import logging
import re

from ncd.global_file import GlobalFile


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class LookupTable(GlobalFile):
    """Helper to import schemaless table files to Athena.

    Args:
        raw_content: A string with the content of a table's text file.
        athena: An ncd.Athena to use when accessing AWS.
    """

    def __init__(self, raw_content=None, athena=None):
        self._raw = raw_content
        self._athena = athena
        self.logger = logger.getChild('LookupTable')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def load(self):
        """Load this table's data into Athena."""
        name = self._extract_table_name()
        table = self._extract_lookup_table()
        self._load_table(name, table)
        self.logger.info('Loaded lookup table {0}'.format(name))

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _extract_lookup_table(self):
        """Extract this table's data from our text file.

        Returns:
            A text file-like object with CSV data for the given table.
        """
        blank_line_matches = tuple(
            re.finditer(r'^[\s\n]*$', self._raw, re.MULTILINE))
        table_start = blank_line_matches[0].end()
        table_end = blank_line_matches[1].start()
        raw_table = self._raw[table_start:table_end].strip()
        return self._extract_global_table(raw_table)

    def _extract_table_name(self):
        """Extract the name of a lookup table from our text file.

        Returns:
            A string table name.
        """
        return re.search(r'(?<=\s)GS_[^\s]+', self._raw).group(0)
