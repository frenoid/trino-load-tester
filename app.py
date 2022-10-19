from pyhive import trino

if __name__ == "__main__": 
  cursor = trino.connect('localhost').cursor()
  cursor.execute('SELECT * FROM tpch.tiny.customer LIMIT 10')
  print(cursor.fetchone())
  print(cursor.fetchall())
