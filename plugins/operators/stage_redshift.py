from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
        region 'us-west-2'
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting existing data from Redshift tables if any")
        postgres_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 bucket to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info(rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, 
            s3_path, 
            credentials.access_key, 
            credentials.secret_key,
            'auto',
        )
        
        postgres_hook.run(formatted_sql)





