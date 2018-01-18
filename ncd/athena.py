from io import BytesIO, TextIOWrapper
import logging
import posixpath
from time import sleep

import boto3


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class Athena(object):
    """Helper for Athena I/O.

    Args:
        data_bucket: A string name of an S3 bucket where we should store data
            files.
        results_bucket: A string name of an S3 bucket where Athena should store
            CSVs of query results.
        s3_prefix: A string prefix to use for data files' S3 keys.
        db_name: A string database name to use when creating and querying
            tables.
    """
    def __init__(
            self, data_bucket=None, results_bucket=None, s3_prefix=None,
            db_name=None):
        self._athena = boto3.client('athena')
        self._s3 = boto3.resource('s3')

        self.data_bucket = data_bucket
        self.results_bucket = results_bucket
        self.s3_prefix = s3_prefix
        self.db_name = db_name

        self.logger = logger.getChild('Athena')

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def create_db(self):
        """Create the database we want to use.
        """
        self.logger.debug('Ensuring Athena database exists')
        self.execute_query(
            'CREATE DATABASE IF NOT EXISTS {0};'.format(self.db_name),
            'default')
        self.logger.debug('CREATE DATABASE query completed')

    def execute_query(self, sql_string, db_name=None):
        """Execute a query on Athena.

        Args:
            sql_string: A string SQL query to execute.

        Returns:
            A StringIO of CSV output from Athena.
        """
        db_for_query = self.db_name if db_name is None else db_name
        start_response = self._athena.start_query_execution(
            QueryString=sql_string,
            QueryExecutionContext={'Database': db_for_query},
            ResultConfiguration={
                'OutputLocation': 's3://{results_bucket}/{s3_prefix}'.format(
                    results_bucket=self.results_bucket,
                    s3_prefix=self.s3_prefix)
            })

        query_execution_id = start_response['QueryExecutionId']
        self.logger.debug('Started query ID {0}'.format(query_execution_id))

        return self._results_for_query(query_execution_id)

    def prefix_for_table(self, table_name):
        """Create a full prefix for S3 keys for a given table.

        Args:
            table_name: A string table name.

        Returns:
            A string S3 prefix.
        """
        return posixpath.join(self.s3_prefix, self.db_name, table_name)

    def upload_data(self, table_name, file_obj, district=None):
        """Upload the given data to S3.

        Args:
            table_name: A string table name.
            file_obj: A binary file-like object.
            district: An optional string code for a federal judicial district;
                provide this when DOJ splits up a table by district.
        """
        if district:
            s3_key = posixpath.join(
                self.prefix_for_table(table_name),
                'filename_district={0}'.format(district),
                '{0}-{1}.json.gz'.format(table_name, district))
        else:
            s3_key = posixpath.join(
                self.prefix_for_table(table_name),
                '{0}.json.gz'.format(table_name))
        file_obj.seek(0)
        self._s3.Bucket(self.data_bucket).upload_fileobj(file_obj, s3_key)

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _results_for_query(self, query_execution_id):
        """Retrieve the results for the given query.

        Args:
            query_execution_id: A string execution ID.

        Returns:
            A text file-like object of CSV output from Athena.
        """
        result_bucket, result_key = self._wait_for_result(query_execution_id)
        results_bytes = BytesIO()
        self.logger.debug('Downloading results for query ID {0}'.format(
            query_execution_id))
        self._s3.Bucket(result_bucket).download_fileobj(
            result_key, results_bytes)
        self.logger.debug('Downloaded results for query ID {0}'.format(
            query_execution_id))
        results_bytes.seek(0)
        results_text = TextIOWrapper(results_bytes, encoding='utf-8')
        return results_text

    def _wait_for_result(self, query_execution_id):
        """Wait for a query to complete.

        This method will block until the query has completed.

        Args:
            query_execution_id: A string execution ID.

        Returns:
            A tuple with two elements:
            *   A string S3 bucket name where results are stored.
            *   A string S3 key for the object containing results.
        """
        try:
            sleep(0.5)
            while True:
                query_execution = self._athena.get_query_execution(
                    QueryExecutionId=query_execution_id)
                query_state = query_execution['QueryExecution'][
                    'Status']['State']
                if query_state != 'RUNNING':
                    break
                self.logger.debug('Waiting for results for query {0}'.format(
                    query_execution_id))
                sleep(5)

            output_location = query_execution['QueryExecution'][
                'ResultConfiguration']['OutputLocation']
            location_components = output_location.split('/', maxsplit=3)

            return (location_components[2], location_components[3])
        except BaseException as e:  # Yes, really.
            self._athena.stop_query_execution(
                QueryExecutionId=query_execution_id)
            raise e
