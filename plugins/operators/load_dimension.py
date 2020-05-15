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
                 mode='append-only',
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.mode = mode
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:
            drop_sql = getattr(SqlQueries, '{}_table_drop'.format(self.table))
            create_sql = getattr(SqlQueries, '{}_table_create'.format(self.table))
            insert_sql = getattr(SqlQueries, '{}_table_insert'.format(self.table))
        except AttributeError:
            raise Exception('sql not defined in sql_queries.py!')

        redshift_hook = PostgresHook(self.redshift_conn_id)
        # drop existing table and data if in delete-load mode
        if self.mode == 'delete-load':
            redshift_hook.run(drop_sql)
        # create_sql is create if not exists, so it supports append mode and will not get error if table hasn't existed.
        redshift_hook.run(create_sql)
        redshift_hook.run(insert_sql)
