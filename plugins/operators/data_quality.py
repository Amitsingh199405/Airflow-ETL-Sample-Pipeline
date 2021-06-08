from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    data_quality_checks = [
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
         'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL",
         'expected_result' : 0}]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        error_count = 0
        failing_tests = []
            
        for check in DataQualityOperator.data_quality_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records = redshift.get_records(sql)[0]
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
            if error_count > 0:
                self.log.info('Data Quality Tests Failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            
            if error_count == 0:
                self.log.info('Data Quality Tests Passed')
