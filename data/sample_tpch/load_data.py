# data/sample_tpch/load_data.py (FULL VERSION)
import psycopg2
import random
from datetime import date, timedelta
import os

def get_host():
    with open("/etc/resolv.conf") as f:
        for line in f:
            if line.startswith("nameserver"):
                return line.split()[1]
    return "localhost"

def main():
    host = get_host()
    print(f"📡 Connecting to PostgreSQL at {host}:5432")
    
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="tpch",
        user="postgres",
        password="querymind123"
    )
    cur = conn.cursor()

    # =============== CREATE FULL SCHEMA ===============
    print("🏗️  Creating TPC-H schema...")
    cur.execute("""
    DROP TABLE IF EXISTS partsupp, lineitem, orders, customer, part, supplier, nation, region;

    CREATE TABLE region (
        r_regionkey INT PRIMARY KEY,
        r_name CHAR(25),
        r_comment VARCHAR(152)
    );

    CREATE TABLE nation (
        n_nationkey INT PRIMARY KEY,
        n_name CHAR(25),
        n_regionkey INT,
        n_comment VARCHAR(152),
        FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
    );

    CREATE TABLE customer (
        c_custkey INT PRIMARY KEY,
        c_name VARCHAR(25),
        c_address VARCHAR(40),
        c_nationkey INT,
        c_phone CHAR(15),
        c_acctbal DECIMAL(15,2),
        c_mktsegment CHAR(10),
        c_comment VARCHAR(117),
        FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
    );

    CREATE TABLE supplier (
        s_suppkey INT PRIMARY KEY,
        s_name CHAR(25),
        s_address VARCHAR(40),
        s_nationkey INT,
        s_phone CHAR(15),
        s_acctbal DECIMAL(15,2),
        s_comment VARCHAR(101),
        FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
    );

    CREATE TABLE part (
        p_partkey INT PRIMARY KEY,
        p_name VARCHAR(55),
        p_mfgr CHAR(25),
        p_brand CHAR(10),
        p_type VARCHAR(25),
        p_size INT,
        p_container CHAR(10),
        p_retailprice DECIMAL(15,2),
        p_comment VARCHAR(23)
    );

    CREATE TABLE partsupp (
        ps_partkey INT,
        ps_suppkey INT,
        ps_availqty INT,
        ps_supplycost DECIMAL(15,2),
        ps_comment VARCHAR(199),
        PRIMARY KEY (ps_partkey, ps_suppkey),
        FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
        FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
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
        o_comment VARCHAR(79),
        FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
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
        PRIMARY KEY (l_orderkey, l_linenumber),
        FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
        FOREIGN KEY (l_partkey) REFERENCES part(p_partkey),
        FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey)
    );
    """)
    print("✅ Schema created")

    # =============== LOAD REALISTIC DATA ===============
    print("🗃️  Loading realistic TPC-H sample data...")

    # Regions (5)
    regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
    for i, name in enumerate(regions):
        cur.execute("INSERT INTO region VALUES (%s, %s, %s);", (i, name, "Comment"))

    # Nations (25)
    nations = [
        ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 0),
        ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
        ("IRAN", 2), ("IRAQ", 0), ("JAPAN", 2), ("JORDAN", 0), ("KENYA", 0),
        ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
        ("SAUDI ARABIA", 0), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1)
    ]
    for i, (name, region) in enumerate(nations):
        cur.execute("INSERT INTO nation VALUES (%s, %s, %s, %s);", (i, name, region, "Comment"))

    # Customers (150)
    segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
    for i in range(150):
        cur.execute("""
            INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            i+1,
            f"CUST{i+1:03d}",
            f"{i} RANDOM STREET",
            random.randint(0, 24),
            f"{random.randint(1000000000, 9999999999)}",
            round(random.uniform(0, 10000), 2),
            random.choice(segments),
            "Comment"
        ))

    # Suppliers (50)
    for i in range(50):
        cur.execute("""
            INSERT INTO supplier VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            i+1,
            f"SUPPLIER#{i+1:03d}",
            f"{i} SUPPLIER STREET",
            random.randint(0, 24),
            f"{random.randint(1000000000, 9999999999)}",
            round(random.uniform(0, 10000), 2),
            "Comment"
        ))

    # Parts (200)
    brands = [f"Brand#{i//50+1}{i%50+1:02d}" for i in range(200)]
    types = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    containers = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "LG CASE", "LG BOX", "LG PACK", "LG PKG"]
    for i in range(200):
        cur.execute("""
            INSERT INTO part VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            i+1,
            f"Part{i+1:03d}",
            "Manufacturer1",
            brands[i],
            random.choice(types),
            random.randint(1, 50),
            random.choice(containers),
            round(random.uniform(100, 2000), 2),
            "Comment"
        ))

    # Partsupp (1000)
    used = set()
    for _ in range(1000):
        while True:
            p = random.randint(1, 200)
            s = random.randint(1, 50)
            if (p, s) not in used:
                used.add((p, s))
                break
        cur.execute("""
            INSERT INTO partsupp VALUES (%s, %s, %s, %s, %s)
        """, (
            p,
            s,
            random.randint(1, 1000),
            round(random.uniform(100, 1000), 2),
            "Comment"
        ))

    # Orders (1500)
    priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
    for i in range(1500):
        order_date = date(1992,1,1) + timedelta(days=random.randint(0, 2556))
        cur.execute("""
            INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            i+1,
            random.randint(1, 150),
            random.choice(["F", "O", "P"]),
            round(random.uniform(1000, 100000), 2),
            order_date,
            random.choice(priorities),
            f"Clerk#{random.randint(1, 1000):03d}",
            random.randint(0, 1),
            "Comment"
        ))

    # Lineitem (6000)
    for i in range(6000):
        order_key = random.randint(1, 1500)
        part_key = random.randint(1, 200)
        supp_key = random.randint(1, 50)
        ship_date = date(1992,1,1) + timedelta(days=random.randint(0, 2556))
        commit_date = ship_date + timedelta(days=random.randint(0, 30))
        receipt_date = commit_date + timedelta(days=random.randint(0, 30))
        cur.execute("""
            INSERT INTO lineitem VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            order_key,
            part_key,
            supp_key,
            i+1,
            random.randint(1, 100),
            round(random.uniform(1000, 50000), 2),
            round(random.uniform(0, 0.1), 2),
            round(random.uniform(0, 0.08), 2),
            random.choice(["A", "N", "R"]),
            random.choice(["F", "O"]),
            ship_date,
            commit_date,
            receipt_date,
            random.choice(["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]),
            random.choice(["RAIL", "TRUCK", "SHIP", "MAIL", "AIR"]),
            "Comment"
        ))

    conn.commit()
    print("✅ TPC-H sample data loaded (150 cust, 50 supp, 200 parts, 1500 orders, 6000 lineitems)")

    # Test query
    cur.execute("""
        SELECT n.n_name, COUNT(*) 
        FROM customer c, nation n 
        WHERE c.c_nationkey = n.n_nationkey 
        GROUP BY n.n_name 
        ORDER BY COUNT(*) DESC 
        LIMIT 3;
    """)
    print("\n🧪 Test query result (top nations by customer count):")
    for row in cur.fetchall():
        print(f"  {row[0]} → {row[1]} customers")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()