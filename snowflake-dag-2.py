from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = 'SnowflakeCon'
SLACK_CONN_ID = 'AirflowSlack'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'MEETUP_DB'
SNOWFLAKE_ROLE = 'SYSADMIN'

slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

slack_msg = 'A data point has been added, updated or deleted and merged into the MEMBERS table!'


with DAG("Snowflake-DAG-2",
    start_date=datetime(2021, 11 ,14), 
    schedule_interval='*/15 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=False) as dag:

# First set of nodes eliminate old rows of information to prepare load from external stage tables from S3 bucket

    snowflake_op_sql_simulate_insert = SnowflakeOperator(
    task_id='snowflake_op_sql_simulate_insert',
    dag=dag,
    sql="CALL simulateInsert('TESTINGSP', 8);",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_merge_members = SnowflakeOperator(
    task_id='snowflake_op_sql_merge_members',
    dag=dag,
    sql="CALL callMerge();",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    slack_alert = SlackWebhookOperator(
        task_id = 'slack_alert',
        http_conn_id = SLACK_CONN_ID,
        webhook_token = slack_webhook_token,
        message = slack_msg,
        username='airflow'
    )

    snowflake_op_sql_simulate_insert >> snowflake_op_sql_merge_members >> slack_alert
    