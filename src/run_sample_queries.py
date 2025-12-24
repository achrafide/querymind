# run_sample_queries.py
import psycopg2

QUERIES = [
    "SELECT COUNT(*) FROM customer;",
    "SELECT c_name FROM customer WHERE c_acctbal > 1000 LIMIT 5;",
    "SELECT COUNT(*) FROM orders WHERE o_orderdate > '1995-01-01';",
    """
    SELECT c.c_name, COUNT(o.o_orderkey)
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c.c_name
    LIMIT 5;
    """,
    "SELECT AVG(l_extendedprice) FROM lineitem;"
]

def run_queries():
    host = open("/etc/resolv.conf").read().split("nameserver")[1].split()[0]
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="tpch",
        user="postgres",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    
    for i, sql in enumerate(QUERIES):
        print(f"Running query {i+1}...")
        cur.execute(sql)
        cur.fetchall()  # consume results
    
    cur.close()
    conn.close()
    print("✅ All queries executed. Data logged in pg_stat_statements.")

if __name__ == "__main__":
    run_queries()