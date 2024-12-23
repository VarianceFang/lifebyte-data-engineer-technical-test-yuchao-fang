import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access credentials
host = os.getenv("PG_HOST")
database = os.getenv("PG_DATABASE")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
port = os.getenv("PG_PORT")

# Establishing the connection
conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password,
    port=port
)

# Get column information for both tables
cur = conn.cursor()
cur.execute("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'trades' ;
""")
columns = cur.fetchall()
print("trades", columns)

cur.execute("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'users' ;
""")
columns = cur.fetchall()
print("users", columns)
cur.close()

# Check for unexpected strings in currency from users
print('Check for unexpected strings in currency from users')
query = "SELECT currency FROM users WHERE currency  !~ '^[a-zA-Z0-9]{1,10}$';"
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected currency found:", result)
else:
    print("No unexpected records found")
    
# Check for unexpected strings in symbol from trades
print('Check for unexpected strings in symbol from trades')
query = "SELECT login_hash,ticket_hash,symbol   FROM trades WHERE symbol !~ '^[a-zA-Z0-9]{1,10}$';"
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected symbols found:", result)
else:
    print("No unexpected records found")

# Check for unexpected value in enable from users
print('Check for unexpected value in enable from users')
query = "SELECT DISTINCT enable FROM users WHERE enable <> 1 and enable <> 0;"
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected symbols found:", result)
else:
    print("No unexpected records found")

# Check for unexpected numerical values in open_price from trades
print('Check for unexpected numerical values in open_price from trades')
query = "SELECT login_hash,ticket_hash, open_price FROM trades WHERE open_price <= 0;"
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected open_price found:", result)
else:
    print("No unexpected records found")

# Check for unexpected numerical values in cmd from trades
print('Check for unexpected numerical values in cmd from trades')
query = "SELECT login_hash,ticket_hash, cmd FROM trades WHERE cmd <> 0 and cmd <> 1;"
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected cmd found:", result)
else:
    print("No unexpected records found")

# Check for unexpected negative numerical values in volume from trades
print('Check for unexpected negative numerical values in volume and contractsize and digits from trades')
query = "SELECT login_hash,ticket_hash, volume FROM trades WHERE volume < 0 OR contractsize < 0 OR digits < 0;"
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result)+ "unexpected volume / contractsize found:", result)
else:
    print("No unexpected records found")

# Check for unexpected decimal places in open_price from trades
print('Check for unexpected decimal places in open_price from trades')
query = """SELECT login_hash,ticket_hash, open_price, digits, LENGTH(SPLIT_PART(open_price::TEXT, '.', 2)) AS decimal_places 
FROM trades
WHERE digits <> LENGTH(SPLIT_PART(open_price::TEXT, '.', 2))
;
"""
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected decimal places in open_price found:")
    cur = conn.cursor()
    cur.execute(query)
    print("Showing first 10 records")
    result = cur.fetchmany(10)
    cur.close()
    print(result)
else:
    print("No unexpected records found")

# Check for joins between trades and users using login_hash and server_hash
print('Check for joins between trades and users using login_hash and server_hash')
query = """SELECT t.login_hash, t.server_hash, t.ticket_hash,t.open_price
FROM trades t
LEFT JOIN users u
ON t.login_hash = u.login_hash AND t.server_hash = u.server_hash
WHERE u.login_hash IS NULL OR u.server_hash IS NULL; 
"""
cur = conn.cursor()
cur.execute(query)
result = cur.fetchall()
cur.close()
if len(result) > 0:
    print(len(result),"unexpected records from trades found:")
    cur = conn.cursor()
    cur.execute(query)
    print("Showing first 10 records")
    result = cur.fetchmany(10)
    cur.close()
    print(result)
else:
    print("No unexpected records found")