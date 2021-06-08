from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    truncate_sql = """
    TRUNCATE TABLE {};
    """
    insert_sql = """
    INSERT INTO {} {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate_data=False,
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.truncate_data=truncate_data
        self.sql_query=sql_query

    def execute(self, context):
        self.log.info(f"LoadFactOperator on {self.table} started. Delete existing data: {self.truncate_data}.")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
     
        if self.truncate_data:
            redshift_hook.run(LoadFactOperator.truncate_sql.format(self.table))
        
        redshift_hook.run(LoadFactOperator.insert_sql.format(self.table, self.sql_query))
        
        self.log.info(f"LoadFactOperator on {self.table} completed.")
