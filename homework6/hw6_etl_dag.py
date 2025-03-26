# Importing 
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

# Defining snowflake connection 
def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

# Creating the stage 
@task 
def create_stage(cursor):
    try: 
        cursor.execute("BEGIN;")
        cursor.execute("""
            CREATE OR REPLACE STAGE dev.raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
            """)
    except Exception as e:
        cursor.execute("ROLLBACK;") 
        print(e)
        raise(e)

# Loading the first table dev.raw.user_session_channel
@task 
def load_channel(cursor):
    try: 
        cursor.execute("BEGIN;")
        # Create table 
        cursor.execute("""
            CREATE OR REPLACE TABLE dev.raw.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'  
            );
            """)
        # Delete from table 
        cursor.execute(f"""DELETE FROM dev.raw.user_session_channel""")

        # Populate table 
        cursor.execute("""
            COPY INTO dev.raw.user_session_channel
            FROM @dev.raw.blob_stage/user_session_channel.csv;
            """)
        
    except Exception as e:
        cursor.execute("ROLLBACK;") 
        print(e)
        raise(e)
    
# Loading the second table dev.raw.session_timestamp
@task 
def load_ts(cursor):
    try: 
        cursor.execute("BEGIN;")
        # Create table 
        cursor.execute("""
            CREATE OR REPLACE TABLE dev.raw.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp  
            );
            """)
        # Delete from table 
        cursor.execute(f"""DELETE FROM dev.raw.session_timestamp""")

        # Populate table 
        cursor.execute("""
            COPY INTO dev.raw.session_timestamp
            FROM @dev.raw.blob_stage/session_timestamp.csv;
            """)
        
    except Exception as e:
        cursor.execute("ROLLBACK;") 
        print(e)
        raise(e)


with DAG(
    dag_id="hw6_etl",
    start_date=datetime(2025, 3, 25),
    catchup=False,
    tags=['hw5', 'ELT'],
    schedule = '10 * * * *'
) as dag:

    cursor = return_snowflake_conn()

    create = create_stage(cursor)
    load1 = load_channel(cursor)
    load2 = load_ts(cursor)

    create >> load1 >> load2