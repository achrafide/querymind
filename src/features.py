# src/features.py
import sqlglot
import psycopg2
import numpy as np

def get_db_host():
    with open("/etc/resolv.conf") as f:
        for line in f:
            if line.startswith("nameserver"):
                return line.split()[1]
    return "localhost"

def get_plan_features(sql):
    """Get raw plan_cost and plan_rows from EXPLAIN"""
    host = get_db_host()
    try:
        conn = psycopg2.connect(
            host=host, port=5432, database="tpch",
            user="postgres", password="querymind123"
        )
        cur = conn.cursor()
        cur.execute("EXPLAIN (FORMAT JSON) " + sql)
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result and isinstance(result[0], list) and len(result[0]) > 0:
            plan = result[0][0]
            cost = plan.get("Plan", {}).get("Total Cost", 1.0)
            rows = plan.get("Plan", {}).get("Plan Rows", 1)
            return max(1.0, cost), max(1, rows)
        else:
            return 1.0, 1
    except:
        return 1.0, 1

def extract_actionable_features(sql):
    """Extract ONLY features that drive optimization actions"""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
        
        # Joins
        n_joins = len(list(ast.find_all(sqlglot.exp.Join)))
        
        # GROUP BY
        has_groupby = 1 if ast.find(sqlglot.exp.Group) else 0
        
        # Filter columns (for index suggestions)
        where = ast.find(sqlglot.exp.Where)
        filter_cols = set()
        if where:
            for col in where.find_all(sqlglot.exp.Column):
                filter_cols.add(col.name)
        n_filter_cols = len(filter_cols)
        
        # Tables (for context)
        tables = {t.name for t in ast.find_all(sqlglot.exp.Table)}
        
        return {
            "n_joins": n_joins,
            "has_groupby": has_groupby,
            "n_filter_cols": n_filter_cols,
            "tables": tables,
            "filter_columns": filter_cols
        }
    except:
        return {
            "n_joins": 0,
            "has_groupby": 0,
            "n_filter_cols": 0,
            "tables": set(),
            "filter_columns": set()
        }

def compute_features(sql):
    """Final feature vector for model + advisor"""
    plan_cost, plan_rows = get_plan_features(sql)
    struct = extract_actionable_features(sql)
    
    return {
        # Model features (order matters!)
        "plan_cost": plan_cost,
        "plan_rows": plan_rows,
        "n_joins": struct["n_joins"],
        "has_groupby": struct["has_groupby"],
        "n_filter_cols": struct["n_filter_cols"],
        
        # Advisor context (not in model)
        "tables": struct["tables"],
        "filter_columns": struct["filter_columns"]
    }