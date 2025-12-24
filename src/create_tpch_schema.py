# create_tpch_schema.py
import psycopg2

SCHEMA = """
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
"""

def create_schema():
    host = open("/etc/resolv.conf").read().split("nameserver")[1].split()[0]
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="tpch",
        user="postgres",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    cur.execute(SCHEMA)
    conn.commit()
    print("✅ TPC-H schema created")
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_schema()