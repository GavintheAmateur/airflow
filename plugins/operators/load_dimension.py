from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries
import logging
class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:
            create_sql = getattr(SqlQueries, '{}_table_create'.format(self.table))
        except AttributeError:
            raise Exception('create sql of the target table not defined!')

        try:
            insert_sql = getattr(SqlQueries, '{}_table_insert'.format(self.table))
        except AttributeError:
            raise Exception('create sql of the target table not defined!')

        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(create_sql)
        redshift_hook.run(insert_sql)