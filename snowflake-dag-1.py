from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from datetime import datetime
from random import randint

SNOWFLAKE_CONN_ID = 'SnowflakeCon'
SLACK_CONN_ID = 'my_slack_conn'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'MEETUP_DB'
SNOWFLAKE_ROLE = 'SYSADMIN'

#SQL statements to run on snowflake



with DAG("Snowflake-DAG-1",
    start_date=datetime(2021, 11 ,14), 
    schedule_interval='*/15 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    catchup=False) as dag:

# First set of nodes eliminate old rows of information to prepare load from external stage tables from S3 bucket

    snowflake_op_sql_truncate_venues = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_venues',
    dag=dag,
    sql="CALL truncateTable('CURATION.VENUES');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_topics = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_topics',
    dag=dag,
    sql="CALL truncateTable('CURATION.TOPICS');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_memberstopics = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_memberstopics',
    dag=dag,
    sql="CALL truncateTable('CURATION.MEMBERS_TOPICS');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_groups = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_groups',
    dag=dag,
    sql="CALL truncateTable('CURATION.GROUPS');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_events = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_events',
    dag=dag,
    sql="CALL truncateTable('CURATION.EVENTS');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_cities = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_cities',
    dag=dag,
    sql="CALL truncateTable('CURATION.CITIES');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )     

    snowflake_op_sql_truncate_categories = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_categories',
    dag=dag,
    sql="CALL truncateTable('CURATION.CATEGORIES');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

# Second set of nodes pull information from external stage S3 into curation tables

    snowflake_op_sql_loadS3_venues = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_venues',
    dag=dag,
    sql="CALL importS3('CURATION.VENUES', 'venues/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_loadS3_topics = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_topics',
    dag=dag,
    sql="CALL importS3('CURATION.TOPICS', 'topics/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_loadS3_memberstopics = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_memberstopics',
    dag=dag,
    sql="CALL importS3('CURATION.MEMBERS_TOPICS', 'members_topics/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_loadS3_groups = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_groups',
    dag=dag,
    sql="CALL importS3('CURATION.GROUPS', 'groups/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_loadS3_events = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_events',
    dag=dag,
    sql="CALL importS3('CURATION.EVENTS', 'events/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_loadS3_cities = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_cities',
    dag=dag,
    sql="CALL importS3('CURATION.CITIES', 'cities/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )     

    snowflake_op_sql_loadS3_categories = SnowflakeOperator(
    task_id='snowflake_op_sql_loadS3_categories',
    dag=dag,
    sql="CALL importS3('CURATION.CATEGORIES', 'categories/');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_eventinfo = SnowflakeOperator(
    task_id='snowflake_op_sql_truncate_eventinfo',
    dag=dag,
    sql="CALL truncateTable('CALCULATION.EVENTINFO');",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_create_eventinfo = SnowflakeOperator(
    task_id='snowflake_op_sql_create_eventinfo',
    dag=dag,
    sql="CALL createEventInfo();",
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema='CURATION',
    role=SNOWFLAKE_ROLE
    )

    snowflake_op_sql_truncate_venues >> snowflake_op_sql_loadS3_venues

    snowflake_op_sql_truncate_topics >> snowflake_op_sql_loadS3_topics

    snowflake_op_sql_truncate_memberstopics >> snowflake_op_sql_loadS3_memberstopics

    snowflake_op_sql_truncate_groups >> snowflake_op_sql_loadS3_groups

    snowflake_op_sql_truncate_events >> snowflake_op_sql_loadS3_events

    snowflake_op_sql_truncate_cities >> snowflake_op_sql_loadS3_cities

    snowflake_op_sql_truncate_categories >> snowflake_op_sql_loadS3_categories

    [snowflake_op_sql_truncate_eventinfo,
    snowflake_op_sql_loadS3_categories,
    snowflake_op_sql_loadS3_cities,
    snowflake_op_sql_loadS3_events,
    snowflake_op_sql_loadS3_groups,
    snowflake_op_sql_loadS3_memberstopics,
    snowflake_op_sql_loadS3_topics,
    snowflake_op_sql_loadS3_venues] >> snowflake_op_sql_create_eventinfo

                       