# scripts/log_queries.py
import psycopg2
import time
import pandas as pd
import sqlglot
from sqlglot import parse_one, transpile, exp

QUERIES = {
    "Q1": "SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, AVG(l_quantity) as avg_qty, AVG(l_extendedprice) as avg_price, AVG(l_discount) as avg_disc, COUNT(*) as count_order FROM lineitem WHERE l_shipdate <= '1998-12-01' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;",
    
    "Q2": "SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment FROM part p, supplier s, partsupp ps, nation n, region r WHERE p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey AND r.r_name = 'EUROPE' AND p.p_size = 15 AND p.p_type LIKE '%BRASS' ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey LIMIT 100;",
    
    "Q3": "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue, o.o_orderdate, o.o_shippriority FROM customer c, orders o, lineitem l WHERE c.c_mktsegment = 'BUILDING' AND c.c_custkey = o.o_custkey AND l.l_orderkey = o.o_orderkey AND l.l_shipdate > '1995-03-15' GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority ORDER BY revenue DESC, o.o_orderdate LIMIT 10;",
    
    "Q4": "SELECT o.o_orderpriority, COUNT(*) as order_count FROM orders o WHERE o.o_orderdate >= '1995-01-01' AND o.o_orderdate < '1996-01-01' AND EXISTS (SELECT * FROM lineitem l WHERE l.l_orderkey = o.o_orderkey AND l.l_commitdate < l.l_receiptdate) GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority;",
    
    "Q5": "SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue FROM customer c, orders o, lineitem l, supplier s, nation n, region r WHERE c.c_custkey = o.o_custkey AND l.l_orderkey = o.o_orderkey AND l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey AND r.r_name = 'ASIA' AND o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1995-01-01' GROUP BY n.n_name ORDER BY revenue DESC;",
    
    "Q6": "SELECT SUM(l.l_extendedprice * l.l_discount) as revenue FROM lineitem l WHERE l.l_shipdate >= '1994-01-01' AND l.l_shipdate < '1995-01-01' AND l.l_discount BETWEEN 0.06 AND 0.08 AND l.l_quantity < 24;",
    
    "Q7": "SELECT supp_nation, cust_nation, l_year, SUM(volume) as revenue FROM (SELECT n1.n_name as supp_nation, n2.n_name as cust_nation, EXTRACT(YEAR FROM l.l_shipdate) as l_year, l.l_extendedprice * (1 - l.l_discount) as volume FROM supplier s, lineitem l, orders o, customer c, nation n1, nation n2 WHERE s.s_suppkey = l.l_suppkey AND l.l_orderkey = o.o_orderkey AND o.o_custkey = c.c_custkey AND c.c_nationkey = n2.n_nationkey AND s.s_nationkey = n1.n_nationkey AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))) as shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;",
    
    "Q8": "SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) as mkt_share FROM (SELECT EXTRACT(YEAR FROM o.o_orderdate) as o_year, l.l_extendedprice * (1 - l.l_discount) as volume, n2.n_name as nation FROM part p, supplier s, lineitem l, orders o, customer c, nation n1, nation n2, region r WHERE p.p_partkey = l.l_partkey AND s.s_suppkey = l.l_suppkey AND l.l_orderkey = o.o_orderkey AND o.o_custkey = c.c_custkey AND c.c_nationkey = n1.n_nationkey AND n1.n_regionkey = r.r_regionkey AND r.r_name = 'AMERICA' AND s.s_nationkey = n2.n_nationkey AND o.o_orderdate BETWEEN '1995-01-01' AND '1996-12-31' AND p.p_type = 'ECONOMY ANODIZED STEEL') as all_nations GROUP BY o_year ORDER BY o_year;",
    
    "Q9": "SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) as amount FROM part p, supplier s, lineitem l, partsupp ps, orders o, nation n WHERE p.p_partkey = l.l_partkey AND s.s_suppkey = l.l_suppkey AND l.l_orderkey = o.o_orderkey AND p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey AND s.s_nationkey = n.n_nationkey AND p.p_name LIKE '%green%' GROUP BY n.n_name ORDER BY amount DESC;",
    
    "Q10": "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment FROM customer c, orders o, lineitem l, nation n WHERE c.c_custkey = o.o_custkey AND l.l_orderkey = o.o_orderkey AND c.c_nationkey = n.n_nationkey AND o.o_orderdate >= '1993-10-01' AND o.o_orderdate < '1994-01-01' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment ORDER BY revenue DESC LIMIT 20;",
    
    "Q11": "SELECT ps.ps_partkey, SUM(ps.ps_supplycost * ps.ps_availqty) as value FROM partsupp ps, supplier s, nation n WHERE ps.ps_suppkey = s.s_suppkey AND s.s_nationkey = n.n_nationkey AND n.n_name = 'GERMANY' GROUP BY ps.ps_partkey HAVING SUM(ps.ps_supplycost * ps.ps_availqty) > (SELECT SUM(ps.ps_supplycost * ps.ps_availqty) * 0.0001 FROM partsupp ps, supplier s, nation n WHERE ps.ps_suppkey = s.s_suppkey AND s.s_nationkey = n.n_nationkey AND n.n_name = 'GERMANY') ORDER BY value DESC;",
    
    "Q12": "SELECT l.l_shipmode, SUM(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count, SUM(CASE WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) as low_line_count FROM orders o, lineitem l WHERE o.o_orderkey = l.l_orderkey AND l.l_shipmode IN ('MAIL', 'SHIP') AND l.l_commitdate < l.l_receiptdate AND l.l_shipdate < l.l_commitdate AND l.l_receiptdate >= '1994-01-01' AND l.l_receiptdate < '1995-01-01' GROUP BY l.l_shipmode ORDER BY l.l_shipmode;",
    
    "Q13": "SELECT c_count, COUNT(*) as custdist FROM (SELECT c.c_custkey, COUNT(o.o_orderkey) as c_count FROM customer c LEFT OUTER JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_comment NOT LIKE '%special%requests%' GROUP BY c.c_custkey) as c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC;",
    
    "Q14": "SELECT 100.00 * SUM(CASE WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END) / SUM(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue FROM lineitem l, part p WHERE l.l_partkey = p.p_partkey AND l.l_shipdate >= '1995-09-01' AND l.l_shipdate < '1995-10-01';",
    
    "Q15": "CREATE VIEW revenue0 AS SELECT l.l_suppkey as supplier_no, SUM(l.l_extendedprice * (1 - l.l_discount)) as total_revenue FROM lineitem l WHERE l.l_shipdate >= '1996-01-01' AND l.l_shipdate < '1996-04-01' GROUP BY l.l_suppkey; SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, r.total_revenue FROM supplier s, revenue0 r WHERE s.s_suppkey = r.supplier_no AND r.total_revenue = (SELECT MAX(total_revenue) FROM revenue0) ORDER BY s.s_suppkey; DROP VIEW revenue0;",
    
    "Q16": "SELECT p.p_brand, p.p_type, p.p_size, COUNT(DISTINCT ps.ps_suppkey) as supplier_cnt FROM part p, partsupp ps WHERE p.p_partkey = ps.ps_partkey AND p.p_brand <> 'Brand#45' AND p.p_type NOT LIKE 'MEDIUM POLISHED%' AND p.p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps.ps_suppkey NOT IN (SELECT s.s_suppkey FROM supplier s WHERE s.s_comment LIKE '%Customer%Complaints%') GROUP BY p.p_brand, p.p_type, p.p_size ORDER BY supplier_cnt DESC, p.p_brand, p.p_type, p.p_size;",
    
    "Q17": "SELECT SUM(l.l_extendedprice) / 7.0 as avg_yearly FROM lineitem l, part p WHERE p.p_partkey = l.l_partkey AND p.p_brand = 'Brand#23' AND p.p_container = 'MED BOX' AND l.l_quantity < (SELECT 0.2 * AVG(l2.l_quantity) FROM lineitem l2 WHERE l2.l_partkey = p.p_partkey);",
    
    "Q18": "SELECT c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, SUM(l.l_quantity) FROM customer c, orders o, lineitem l WHERE c.c_custkey = o.o_custkey AND o.o_orderkey = l.l_orderkey GROUP BY c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice HAVING SUM(l.l_quantity) > 300 ORDER BY o.o_totalprice DESC, o.o_orderdate LIMIT 100;",
    
    "Q19": "SELECT SUM(l.l_extendedprice* (1 - l.l_discount)) as revenue FROM lineitem l, part p WHERE p.p_partkey = l.l_partkey AND l.l_shipmode IN ('AIR', 'AIR REG') AND l.l_shipinstruct = 'DELIVER IN PERSON' AND ((p.p_brand = 'Brand#12' AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l.l_quantity >= 1 AND l.l_quantity <= 11 AND p.p_size BETWEEN 1 AND 5) OR (p.p_brand = 'Brand#23' AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l.l_quantity >= 10 AND l.l_quantity <= 20 AND p.p_size BETWEEN 1 AND 10) OR (p.p_brand = 'Brand#34' AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l.l_quantity >= 20 AND l.l_quantity <= 30 AND p.p_size BETWEEN 1 AND 15));",
    
    "Q20": "SELECT s.s_name, s.s_address FROM supplier s, nation n WHERE s.s_suppkey IN (SELECT ps.ps_suppkey FROM partsupp ps WHERE ps.ps_partkey IN (SELECT p.p_partkey FROM part p WHERE p.p_name LIKE 'forest%') AND ps.ps_availqty > (SELECT 0.5 * SUM(l.l_quantity) FROM lineitem l WHERE l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey AND l.l_shipdate >= '1994-01-01' AND l.l_shipdate < '1995-01-01')) AND s.s_nationkey = n.n_nationkey AND n.n_name = 'CANADA' ORDER BY s.s_name;",
    
    "Q21": "SELECT s.s_name, COUNT(*) as numwait FROM supplier s, lineitem l1, orders o, nation n WHERE s.s_suppkey = l1.l_suppkey AND o.o_orderkey = l1.l_orderkey AND o.o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s.s_nationkey = n.n_nationkey AND n.n_name = 'SAUDI ARABIA' GROUP BY s.s_name ORDER BY numwait DESC, s.s_name LIMIT 100;",
    
    "Q22": "SELECT cntrycode, COUNT(*) as numcust, SUM(custsale.c_acctbal) as totacctbal FROM (SELECT SUBSTRING(c.c_phone, 1, 2) as cntrycode, c.c_acctbal FROM customer c WHERE SUBSTRING(c.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2 WHERE c2.c_acctbal > 0.00 AND SUBSTRING(c2.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS (SELECT * FROM orders o WHERE o.o_custkey = c.c_custkey)) as custsale GROUP BY cntrycode ORDER BY cntrycode;"
}

def get_host():
    with open("/etc/resolv.conf") as f:
        for line in f:
            if line.startswith("nameserver"):
                return line.split()[1]
    return "localhost"

def extract_features(sql):
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
        tables = {table.name for table in ast.find_all(sqlglot.exp.Table)}
        joins = len(list(ast.find_all(sqlglot.exp.Join)))
        where = ast.find(sqlglot.exp.Where)
        filters = str(where).count(" = ") if where else 0
        return {
            "n_tables": len(tables),
            "n_joins": joins,
            "n_filters": filters,
            "has_groupby": 1 if ast.find(sqlglot.exp.Group) else 0,
            "has_orderby": 1 if ast.find(sqlglot.exp.Order) else 0
        }
    except Exception as e:
        print(f"Parse error for SQL: {sql[:50]}... → {e}")
        return {"n_tables": 0, "n_joins": 0, "n_filters": 0, "has_groupby": 0, "has_orderby": 0}

def main():
    host = get_host()
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="tpch",
        user="postgres",
        password="querymind123"
    )
    cur = conn.cursor()

    # Ensure pg_stat_statements is available
    try:
        cur.execute("SELECT query FROM pg_stat_statements LIMIT 1;")
    except psycopg2.errors.UndefinedTable:
        print("⚠️  pg_stat_statements not enabled. Ensure 'shared_preload_libraries=pg_stat_statements' in docker-compose.yml and restart.")
        return

    logs = []
    for q_id, sql_text in QUERIES.items():
        print(f"Running {q_id}...")
        start = time.time()

        # Handle multi-statement queries (like Q15)
        success = True
        error_msg = None
        rows_returned = 0
        try:
            if ";" in sql_text.strip() and sql_text.strip()[-1] == ";":
                statements = [s.strip() for s in sql_text.strip().rstrip(";").split(";") if s.strip()]
                for stmt in statements:
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        success = False
                        error_msg = str(e)
                        print(f"Error executing {q_id} statement: {e}")
                        break
                    if stmt.strip().upper().startswith("SELECT"):
                        try:
                            rows = cur.fetchall()
                            rows_returned += len(rows)
                        except psycopg2.ProgrammingError:
                            pass
            else:
                if sql_text.strip().upper().startswith("SELECT"):
                    # For SELECT queries, also run EXPLAIN
                    try:
                        cur.execute("EXPLAIN (FORMAT JSON) " + sql_text)
                        explain_result = cur.fetchone()
                        if explain_result is None:
                            plan_cost = 0
                            plan_rows = 0
                        else:
                            plan_json = explain_result[0]
                            if isinstance(plan_json, list) and len(plan_json) > 0:
                                plan = plan_json[0]
                                plan_cost = plan.get("Plan", {}).get("Total Cost", 0)
                                plan_rows = plan.get("Plan", {}).get("Plan Rows", 0)
                            else:
                                plan_cost = 0
                                plan_rows = 0
                    except Exception as e:
                        print(f"EXPLAIN failed for {q_id}: {e}")
                        plan_cost = 0
                        plan_rows = 0

                    # Now execute the actual query
                    try:
                        cur.execute(sql_text)
                        rows = cur.fetchall()
                        rows_returned = len(rows)
                    except Exception as e:
                        success = False
                        error_msg = str(e)
                        print(f"Error executing {q_id}: {e}")
                        plan_cost = 0
                        plan_rows = 0
                else:
                    # Non-SELECT (e.g., CREATE VIEW)
                    try:
                        cur.execute(sql_text)
                    except Exception as e:
                        success = False
                        error_msg = str(e)
                        print(f"Error executing {q_id}: {e}")
            
        except Exception as e:
            success = False
            error_msg = str(e)
            print(f"Unexpected error executing {q_id}: {e}")
            plan_cost = 0
            plan_rows = 0

        runtime_ms = (time.time() - start) * 1000
        
        # Record runtime, success/error info and extracted features
        feats = extract_features(sql_text)
        logs.append({
            "query_id": q_id,
            "sql": sql_text,
            "runtime_ms": runtime_ms,
            "success": success,
            "error": error_msg,
            "rows_returned": rows_returned,
            "plan_cost": plan_cost if 'plan_cost' in locals() else 0,
            "plan_rows": plan_rows if 'plan_rows' in locals() else 0,
            **feats
        })
        

    cur.close()
    conn.close()

    df = pd.DataFrame(logs)
    df.to_csv("data/query_log.csv", index=False)
    print(f"\n✅ Success! Dataset saved to data/query_log.csv")
    print(df[["query_id", "runtime_ms", "n_tables", "n_joins", "plan_cost", "plan_rows"]])

if __name__ == "__main__":
    main()