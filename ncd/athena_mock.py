from datetime import datetime
import posixpath
from pathlib import Path
from shutil import copyfileobj


class AthenaMock(object):
    """Mock for ncd.athena.Athena that saves to disk.

    Args:
        data_bucket: Ignored.
        results_bucket: Ignored.
        s3_prefix: A string base directory into which table data and queries
            will be saved.
    """
    def __init__(self, data_bucket=None, results_bucket=None, s3_prefix=None):
        self.data_bucket = data_bucket
        self.results_bucket = results_bucket
        self.s3_prefix = s3_prefix

        self._base_dir = Path(s3_prefix)

        self._query_dir = self._base_dir / 'queries'
        self._table_dir = self._base_dir / 'tables'

        self._query_dir.mkdir(parents=True, exist_ok=True)
        self._table_dir.mkdir(parents=True, exist_ok=True)

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def execute_query(self, sql_string):
        """Save the given query to disk.

        Args:
            sql_string: A string SQL query to save.
        """
        timestamp = datetime.now().isoformat()
        output_path = self._query_dir / '{0}.sql'.format(timestamp)
        with open(output_path, 'w') as output_file:
            output_file.write(sql_string)

    def prefix_for_table(self, table_name):
        """Create a full prefix for S3 keys for a given table.

        Args:
            table_name: A string table name.

        Returns:
            A string S3 prefix.
        """
        return posixpath.join(str(self._base_dir), table_name)

    def upload_data(self, table_name, file_obj):
        """Save the given data to disk.

        Args:
            table_name: A string table name.
            file_obj: A binary file-like object.
        """
        output_path = self._table_dir / table_name / '{0}.json.gz'.format(
            table_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'wb') as output_file:
            copyfileobj(file_obj, output_file)

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # TODO: Add this.
