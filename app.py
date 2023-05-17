from time import sleep, time
from pyhive import trino
from uuid import uuid4
from os import getenv
import logging

ALLOWABLE_MODES = ("ctas", "select", "tpcds")
MAX_TPCDS_QUERY_NUMBER = 99 # Number of queries
DUMMY_QUERY = "SELECT * FROM tpch.sf100.customer LIMIT 10"

MODE = getenv("MODE")

TRINO_HOST = getenv("TRINO_HOST")
TRINO_USER = getenv("TRINO_USER")
TRINO_PORT= getenv("TRINO_PORT", 8080)
TRINO_CATALOG = getenv("TRINO_CATALOG", "hive")

SCHEMA_NAME = getenv("SCHEMA_NAME")
SCHEMA_BUCKET = getenv("SCHEMA_BUCKET")
TABLE_PREFIX = getenv("TABLE_PREFIX", "test")
TABLE_COUNT = int(getenv("TABLE_COUNT", 5))

TPCDS_SCHEMA = getenv("TPCDS_SCHEMA", "sf100")
SLEEP_INTERVAL = int(getenv("SLEEP_INTERVAL", 3))
MAX_RETRY = int(getenv("MAX_RETRY", 3)) # 0 means no limit to retries

def create_schema(cursor, schema_name: str, bucket_name: str) -> None:
  cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}  WITH (LOCATION = 's3a://{bucket_name}/{schema_name}')")
  data = cursor.fetchall()

  return

def drop_schema(cursor, schema_name: str) -> None:
  cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name}")
  data = cursor.fetchall()

  return

def generate_table_name(prefix: str) -> str:
  return prefix + "_" + str(uuid4())[:8]

def create_table(cursor, schema_name: str, source_table: str, destination_table: str) -> None:
  cursor.execute(f"CREATE TABLE {schema_name}.{destination_table} AS SELECT * FROM {source_table}")
  data = cursor.fetchall()

  return

def drop_table(cursor, schema_name: str, table_name: str) -> None:
  cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
  data = cursor.fetchall()

  return

def query_and_fetchall(cursor, query: str) -> list:
  cursor.execute(query)
  return cursor.fetchall() 

def get_tpcds_query(number: int) -> str:
  with open(f"sql/tpcds/query{number}.sql") as f:
    return f.read().replace(";", "")
  
def query_and_fetchall_with_retry(cursor, query: str, backoff_seconds: int, max_retry) -> list:
  tries = 0
  query_start_time = time()
  while (tries < max_retry) or max_retry == 0:
    logging.info(f"*********************** Try #{tries} ***************************")
    try:
      cursor.execute(query)
      logging.info(f"Query executed after {time() - query_start_time} seconds")
      results = cursor.fetchall()
      logging.info(f"Results returned after {time() - query_start_time} seconds")
      # Break while loop if successful
      break
    except Exception as e:
      logging.info(e)
    finally:
      tries += 1
    sleep(backoff_seconds)

  return results
  



if __name__ == "__main__":
  logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
  logging.info("* START")

  
  if MODE not in ALLOWABLE_MODES:
    logging.info(f"Unknown mode: {MODE}")
    logging.info(f"allowable modes: {ALLOWABLE_MODES}")

    exit(1)
  
  if MODE == "ctas":
    logging.info("ctas mode")
    cursor = trino.connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    username=TRINO_USER,
    catalog=TRINO_CATALOG).cursor()

    logging.info(query_and_fetchall(cursor, DUMMY_QUERY))

    logging.info("Creating a schema")
    create_schema(cursor=cursor, schema_name=SCHEMA_NAME, bucket_name=SCHEMA_BUCKET)

    created_tables = []
    try:
      while True:
        for i in range(TABLE_COUNT):
          logging.info(f"* CREATING TABLE {i+1}")
          created_table_name = generate_table_name(prefix=TABLE_PREFIX)
          create_table(cursor=cursor, schema_name=SCHEMA_NAME, source_table="tpch.tiny.customer", destination_table=created_table_name)
          created_tables.append(created_table_name)

        for i, table_name in enumerate(created_tables):
          logging.info(f"* DROPPING TABLE {i+1}")
          drop_table(cursor=cursor, schema_name=SCHEMA_NAME, table_name=table_name)

        logging.info(f"* SLEEP {SLEEP_INTERVAL} seconds")
        sleep(SLEEP_INTERVAL)
    except Exception as e:
      logging.info(e)
    finally:
      for i, table_name in enumerate(created_tables):
          logging.info(f"* CLEANUP: DROPPING TABLE {i+1}")
          drop_table(cursor=cursor, schema_name=SCHEMA_NAME, table_name=table_name)
    
  elif MODE == "select":
    print (f"{MODE} MODE")
    cursor = trino.connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    username=TRINO_USER,
    catalog=TRINO_CATALOG).cursor()
    
    while True:
      logging.info(query_and_fetchall(cursor, DUMMY_QUERY))
      sleep(SLEEP_INTERVAL)

  elif MODE == "tpcds":
    print (f"{MODE} MODE")
    cursor = trino.connect(
      host=TRINO_HOST,
      port=TRINO_PORT,
      username=TRINO_USER,
      schema=TPCDS_SCHEMA,
      catalog=TRINO_CATALOG).cursor()
    
    total_completed_queries = 0

    # Keep querying
    while True:
      for query_number in range(1, MAX_TPCDS_QUERY_NUMBER + 1):
        logging.info(f"*** Preparing query #{query_number}")
        query = get_tpcds_query(number=query_number)
        logging.info("Sending query")
        query_and_fetchall_with_retry(cursor=cursor, query=query, backoff_seconds=SLEEP_INTERVAL, max_retry=MAX_RETRY)
        logging.info(f"$$$ Completed query #{query_number}")
        logging.info(f"Completed queries: {total_completed_queries}")


  else:
    logging.info("Unknown mode")

  exit(0)