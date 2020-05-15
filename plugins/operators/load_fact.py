from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        super(LoadFactOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(SqlQueries.songplay_table_insert)

