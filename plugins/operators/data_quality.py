from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        super(DataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(SqlQueries.quality_check_1)