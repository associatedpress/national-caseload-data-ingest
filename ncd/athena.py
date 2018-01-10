# flake8: noqa
import boto3


# Transcript of experimentation with a `SELECT 1;` query:
# >>> athena = boto3.client('athena')
# >>> start_response = client.start_query_execution(QueryString='SELECT 1;', QueryExecutionContext={'Database': 'ncd_test'})
# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
# NameError: name 'client' is not defined
# >>> start_response = athena.start_query_execution(QueryString='SELECT 1;', QueryExecutionContext={'Database': 'ncd_test'})
# Traceback (most recent call last):
#   [...]
#   File "/Users/idguser/.virtualenvs/ncd/lib/python3.5/site-packages/botocore/validate.py", line 291, in serialize_to_request
#     raise ParamValidationError(report=report.generate_report())
# botocore.exceptions.ParamValidationError: Parameter validation failed:
# Missing required parameter in input: "ResultConfiguration"
# >>> start_response = athena.start_query_execution(QueryString='SELECT 1;', QueryExecutionContext={'Database': 'ncd_test'}, ResultConfiguration={'OutputLocation': 's3://aws-athena-query-results-462561078695-us-east-2/query_test'})
# Traceback (most recent call last):
#   [...]
#   File "/Users/idguser/.virtualenvs/ncd/lib/python3.5/site-packages/botocore/client.py", line 615, in _make_api_call
#     raise error_class(parsed_response, operation_name)
# botocore.errorfactory.InvalidRequestException: An error occurred (InvalidRequestException) when calling the StartQueryExecution operation: The S3 location provided to save your query results is invalid. Please check your S3 location is correct and is in the same region and try again. If you continue to see the issue, contact customer support for further assistance.
# >>> start_response = athena.start_query_execution(QueryString='SELECT 1;', QueryExecutionContext={'Database': 'ncd_test'}, ResultConfiguration={'OutputLocation': 's3://aws-athena-query-results-462561078695-us-east-1/query_test'})
# >>> start_response
# {'ResponseMetadata': {'RequestId': '6020fd71-f62f-11e7-8b99-8dd87ec67ff5', 'HTTPStatusCode': 200, 'RetryAttempts': 0, 'HTTPHeaders': {'date': 'Wed, 10 Jan 2018 17:54:59 GMT', 'content-type': 'application/x-amz-json-1.1', 'connection': 'keep-alive', 'content-length': '59', 'x-amzn-requestid': '6020fd71-f62f-11e7-8b99-8dd87ec67ff5'}}, 'QueryExecutionId': 'ba61e860-6964-4e84-baa9-996a7f20fadb'}
# >>> s3 = boto3.client('s3')
# >>> s3.waiter_names
# ['bucket_exists', 'bucket_not_exists', 'object_exists', 'object_not_exists']
# >>> s3_object_exists_waiter = s3.get_waiter('object_exists')
# >>> s3_object_exists_waiter.wait(Bucket='aws-athena-query-results-462561078695-us-east-1', Key='query_test/ba61e860-6964-4e84-baa9-996a7f20fadb.csv'
# ... )
# >>> s3_object_exists_waiter.wait(Bucket='aws-athena-query-results-462561078695-us-east-1', Key='query_test/ba61e860-6964-4e84-baa9-996a7f20fadb.csv.foo')
# ^CTraceback (most recent call last):
#   File "<stdin>", line 1, in <module>
#   File "/Users/idguser/.virtualenvs/ncd/lib/python3.5/site-packages/botocore/waiter.py", line 53, in wait
#     Waiter.wait(self, **kwargs)
#   File "/Users/idguser/.virtualenvs/ncd/lib/python3.5/site-packages/botocore/waiter.py", line 331, in wait
#     time.sleep(sleep_amount)
# KeyboardInterrupt
# >>>


class Athena(object):
    """TODO: Document this.
    """
    def __init__(self, data_bucket=None, results_bucket=None, s3_prefix=None):
        self._athena = boto3.client('athena')
        self._s3 = boto3.client('s3')
        self.data_bucket = data_bucket
        self.results_bucket = results_bucket
        self.s3_prefix = s3_prefix

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-= PUBLIC METHODS FOLLOW =-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def execute_query(self, sql_string):
        """TODO: Document this.
        """
        pass

    def prefix_for_table(self):
        """TODO: Document this.
        """
        pass

    def upload_data(self, table_name, file_obj):
        """TODO: Document this.
        """
        pass

    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=- INTERNAL METHODS FOLLOW -=-=-=-=-=-=-=-=-=-=-=-=-=-
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    def _wait_for_result(self, query_execution_id):
        """TODO: Document this.
        """
        pass
