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
        db_name: Ignored.
    """
    def __init__(
            self, data_bucket=None, results_bucket=None, s3_prefix=None,
            db_name=None):
        self.data_bucket = data_bucket
        self.results_bucket = results_bucket
        self.s3_prefix = s3_prefix
        self.db_name = db_name

        self._base_dir = Path(s3_prefix)

        self._query_dir = self._base_dir / 'queries'
        self._table_dir = self._base_dir / 'tables'

        self._query_dir.mkdir(parents=True, exist_ok=True)
        self._table_dir.mkdir(parents=True, exist_ok=True)

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def create_db(self):
        """No-op.
        """
        return

    def execute_query(self, sql_string):
        """Save the given query to disk.

        Args:
            sql_string: A string SQL query to save.
        """
        timestamp = datetime.now().isoformat()
        output_path = self._query_dir / '{0}.sql'.format(timestamp)
        with output_path.open('w') as output_file:
            output_file.write(sql_string)

    def prefix_for_table(self, table_name):
        """Create a full prefix for S3 keys for a given table.

        Args:
            table_name: A string table name.

        Returns:
            A string S3 prefix.
        """
        return posixpath.join(str(self._base_dir), table_name)

    def upload_data(self, table_name, file_obj, district=None):
        """Save the given data to disk.

        Args:
            table_name: A string table name.
            file_obj: A binary file-like object.
            district: An optional string code for a federal judicial district;
                provide this when DOJ splits up a table by district.
        """
        if district:
            table_dir = self._table_dir / table_name
            district_dir = table_dir / 'filename_district={0}'.format(district)
            output_path = district_dir / '{0}-{1}.json.gz'.format(
                table_name, district)
        else:
            output_path = self._table_dir / table_name / '{0}.json.gz'.format(
                table_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        file_obj.seek(0)
        with output_path.open('wb') as output_file:
            copyfileobj(file_obj, output_file)

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # TODO: Add this.
