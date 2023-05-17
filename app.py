from time import sleep
from webbrowser import get
from pyhive import trino
from uuid import uuid4
from os import getenv

TRINO_HOST = getenv("TRINO_HOST")
TRINO_USER = getenv("TRINO_USER")
TRINO_PORT= getenv("TRINO_PORT", 8080)
TRINO_CATALOG = getenv("TRINO_CATALOG", "hive")
MODE = getenv("MODE")
SCHEMA_NAME = getenv("SCHEMA_NAME")
SCHEMA_BUCKET = getenv("SCHEMA_BUCKET")
TABLE_PREFIX = getenv("TABLE_PREFIX", "test")
TABLE_COUNT = int(getenv("TABLE_COUNT", 5))
SLEEP_INTERVAL = int(getenv("SLEEP_INTERVAL", 3))

ALLOWABLE_MODES = ("ctas", "select")


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

if __name__ == "__main__":
  print("* START")

  cursor = trino.connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    username=TRINO_USER,
    catalog=TRINO_CATALOG).cursor()
  
  if MODE not in ALLOWABLE_MODES:
    print(f"Unknown mode: {MODE}")
    print(f"allowable modes: {ALLOWABLE_MODES}")

    exit(1)
  
  if MODE == "ctas":
    print("ctas mode")
    print(query_and_fetchall(cursor, 'SELECT * FROM tpch.sf100.customer LIMIT 10'))

    print("Creating a schema")
    create_schema(cursor=cursor, schema_name=SCHEMA_NAME, bucket_name=SCHEMA_BUCKET)

    created_tables = []
    try:
      while True:
        for i in range(TABLE_COUNT):
          print(f"* CREATING TABLE {i+1}")
          created_table_name = generate_table_name(prefix=TABLE_PREFIX)
          create_table(cursor=cursor, schema_name=SCHEMA_NAME, source_table="tpch.tiny.customer", destination_table=created_table_name)
          created_tables.append(created_table_name)

        for i, table_name in enumerate(created_tables):
          print(f"* DROPPING TABLE {i+1}")
          drop_table(cursor=cursor, schema_name=SCHEMA_NAME, table_name=table_name)

        print(f"* SLEEP {SLEEP_INTERVAL} seconds")
        sleep(SLEEP_INTERVAL)
    except Exception as e:
      print(e)
    finally:
      for i, table_name in enumerate(created_tables):
          print(f"* CLEANUP: DROPPING TABLE {i+1}")
          drop_table(cursor=cursor, schema_name=SCHEMA_NAME, table_name=table_name)
    
  elif MODE == "select":
    print ("select MODE")
    while True:
      print(query_and_fetchall(cursor, 'SELECT * FROM tpch.sf100.customer LIMIT 100'))
      sleep(SLEEP_INTERVAL)
  else:
    print("Unknown mode")

  exit(0)