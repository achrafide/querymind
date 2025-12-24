# create_and_load_tpch.py
import psycopg2
from datetime import date

def get_docker_host():
    """Get Windows host IP from WSL2"""
    try:
        with open("/etc/resolv.conf") as f:
            for line in f:
                if line.startswith("nameserver"):
                    return line.split()[1]
    except:
        pass
    return "localhost"

def create_schema(cur):
    print("Creating TPC-H tables...")
    
    # Drop tables if exist (for clean restart)
    cur.execute("DROP TABLE IF EXISTS lineitem, orders, customer;")
    
    cur.execute("""
    CREATE TABLE customer (
        c_custkey INT PRIMARY KEY,
        c_name VARCHAR(25),
        c_address VARCHAR(40),
        c_nationkey INT,
        c_phone CHAR(15),
        c_acctbal DECIMAL(15,2),
        c_mktsegment CHAR(10),
        c_comment VARCHAR(117)
    );
    """)
    
    cur.execute("""
    CREATE TABLE orders (
        o_orderkey INT PRIMARY KEY,
        o_custkey INT,
        o_orderstatus CHAR(1),
        o_totalprice DECIMAL(15,2),
        o_orderdate DATE,
        o_orderpriority CHAR(15),
        o_clerk CHAR(15),
        o_shippriority INT,
        o_comment VARCHAR(79)
    );
    """)
    
    cur.execute("""
    CREATE TABLE lineitem (
        l_orderkey INT,
        l_partkey INT,
        l_suppkey INT,
        l_linenumber INT,
        l_quantity DECIMAL(15,2),
        l_extendedprice DECIMAL(15,2),
        l_discount DECIMAL(15,2),
        l_tax DECIMAL(15,2),
        l_returnflag CHAR(1),
        l_linestatus CHAR(1),
        l_shipdate DATE,
        l_commitdate DATE,
        l_receiptdate DATE,
        l_shipinstruct CHAR(25),
        l_shipmode CHAR(10),
        l_comment VARCHAR(44),
        PRIMARY KEY (l_orderkey, l_linenumber)
    );
    """)
    print("✅ Schema created")

def load_data(cur):
    print("Loading sample data...")
    
    # Customers
    customers = [
        (1, 'Customer#000000001', 'KwYVHQxV', 15, '227-645-3358', 3570.00, 'BUILDING', 'regular requests haggle'),
        (2, 'Customer#000000002', 'q9ZV9Z4h', 5, '197-193-3223', 7980.00, 'AUTOMOBILE', 'requests sleep. final packages'),
        (3, 'Customer#000000003', 'iEYh8r8N', 8, '153-555-1870', -870.00, 'MACHINERY', 'final excuses about the ironic'),
    ]
    cur.executemany("""
        INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, customers)
    
    # Orders
    orders = [
        (1, 1, 'O', 173665.03, date(1996,1,2), '5-LOW', 'Clerk#000000951', 0, 'final accounts'),
        (2, 2, 'F', 311480.08, date(1996,5,15), '1-URGENT', 'Clerk#000000124', 0, 'requests sleep'),
        (3, 3, 'O', 301299.99, date(1996,3,30), '5-LOW', 'Clerk#000000019', 0, 'blithely bold'),
    ]
    cur.executemany("""
        INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, orders)
    
    # Lineitems
    lineitems = [
        (1, 1, 1, 1, 17.00, 21168.23, 0.04, 0.02, 'N', 'O', date(1996,3,13), date(1996,2,28), date(1996,3,22), 'NONE', 'RAIL', 'regular requests'),
        (1, 2, 2, 2, 36.00, 45982.20, 0.10, 0.05, 'A', 'F', date(1996,4,12), date(1996,2,20), date(1996,4,22), 'TAKE BACK RETURN', 'MAIL', 'final excuses'),
        (2, 3, 3, 1, 20.00, 31148.01, 0.08, 0.04, 'N', 'O', date(1996,6,10), date(1996,5,1), date(1996,6,20), 'NONE', 'AIR', 'final accounts'),
    ]
    cur.executemany("""
        INSERT INTO lineitem VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, lineitems)
    
    print("✅ Sample data loaded")

def main():
    host = get_docker_host()
    print(f"📡 Connecting to PostgreSQL at {host}:5432")
    
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="tpch",
        user="postgres",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    
    create_schema(cur)
    load_data(cur)
    
    conn.commit()
    cur.close()
    conn.close()
    print("\n🎉 TPC-H database ready!")

if __name__ == "__main__":
    main()