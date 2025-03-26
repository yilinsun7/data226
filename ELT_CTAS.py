# Importing 
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import logging


# Defining snowflake connection 
def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

# The join task that joins two user tables 
@task
def run_ctas(database, schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute(f"DROP TABLE IF EXISTS {database}.{schema}.temp_{table}")
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
        # Check for duplicate records: 
        check_sql = f"""
                    SELECT COUNT(*) AS total_count, COUNT(DISTINCT *) AS distinct_count
                    FROM {database}.{schema}.temp_{table}"""
        cur.execute(check_sql)
        result = cur.fetchone()
        if result and result[0] != result[1]:
            raise Exception(f"Duplicate records detected: Total={result[0]}, Distinct={result[1]}")
            
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id = 'BuildELT_CTAS',
    start_date = datetime(2025, 3, 25),
    catchup=False,
    tags=['ELT', 'hw5'],
    schedule = '45 2 * * *'
) as dag:

    database = "dev"
    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM dev.raw.user_session_channel u
    JOIN dev.raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(database, schema, table, select_sql, primary_key='sessionId')