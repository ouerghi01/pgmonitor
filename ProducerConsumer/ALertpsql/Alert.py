import psycopg2 
import re 
HOST="localhost"
PORT="5432"
USER="aziz"
DBNAME="bench"
conn=psycopg2.connect(

    database=DBNAME,
    host=HOST,
    port=PORT,
    user=USER,
    password="000"  # replace with your postgres password
)
if not conn:
    print("Unable to connect to the database.")
else:
    print("Connected to the database successfully.")
# find the query that give the size of the database
manual_query=f"SELECT pg_size_pretty(pg_database_size('{DBNAME}'));"
cur=conn.cursor()
cur.execute(manual_query)
size_db=cur.fetchone()
list_match=re.findall(r'\d+',size_db[0])
print(f"Size of {DBNAME} database: {size_db[0]}")
if (int(list_match[0])>36):
    print(f"The size of {DBNAME} database is larger than 36 MB.")
