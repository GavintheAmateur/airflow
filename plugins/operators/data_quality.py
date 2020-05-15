from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tests,
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        super(DataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for key,value in self.tests:
            dq_check_sql = key
            expected_result = value
            if expected_result !=  redshift_hook.run(dq_check_sql):
                raise Exception("Data quality check failed. Failing this DAG run. \
                                The failed quality check query is: {}".format(dq_check_sql))