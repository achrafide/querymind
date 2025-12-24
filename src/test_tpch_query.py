# test_tpch_query.py
import psycopg2

def get_docker_host():
    with open("/etc/resolv.conf") as f:
        return [line for line in f if line.startswith("nameserver")][0].split()[1]

host = get_docker_host()
conn = psycopg2.connect(
    host=host,
    port=5432,
    database="tpch",
    user="postgres",
    password="mysecretpassword"
)
cur = conn.cursor()

cur.execute("""
    SELECT c.c_name, COUNT(o.o_orderkey)
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c.c_name;
""")

print("Customer orders:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} orders")

cur.close()
conn.close()