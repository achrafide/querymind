#!/usr/bin/env python3
# src/advisor/predict.py
import sys
import os
import joblib
import numpy as np
import pandas as pd

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from src.features import compute_features

def main():
    if len(sys.argv) < 2:
        print("Usage: python predict.py \"SELECT ...\"")
        return
    
    # Check for --optimize flag
    if "--optimize" in sys.argv:
        sql = sys.argv[1] if sys.argv[1] != "--optimize" else sys.argv[2]
        try:
            from src.optimizer.rewriter import rewrite_query
            rewrite_result = rewrite_query(sql)
            print("✅ Optimized Query:")
            print(rewrite_result["optimized"])
            print("\n💡 Improvements:")
            for imp in rewrite_result["improvements"]:
                print(f"   - {imp}")
        except Exception as e:
            print(f"Optimization failed: {e}")
        return

    # Default: Predict runtime
    sql = sys.argv[1]

    # Load model and config
    model_path = os.path.join(os.path.dirname(__file__), "..", "..", "models", "runtime_predictor_final.joblib")
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "models", "feature_config.joblib")
    
    model = joblib.load(model_path)
    config = joblib.load(config_path)

    # Compute features using SHARED logic
    feats = compute_features(sql)

    # Build feature vector EXACTLY as in training (4 structural features)
    feature_cols = config["feature_cols"]
    X = pd.DataFrame([[
        feats["n_joins"],
        feats["has_groupby"],
        feats["n_filter_cols"],
        feats["plan_rows"]
    ]], columns=feature_cols)

    # Predict raw runtime (no log/expm1)
    pred_ms = model.predict(X)[0]

    print(f"⏱️  Predicted runtime: {pred_ms:.1f} ms")

    # === ACTIONABLE SUGGESTIONS (based on features) ===
    suggestions = []

    # Rule 1: ALWAYS suggest indexes for join queries (even if fast)
    if feats["n_joins"] >= 1:  # Changed from >=2 to >=1
        if "customer" in feats["tables"] and "c_custkey" in feats["filter_columns"]:
            suggestions.append("CREATE INDEX ON customer(c_custkey);")
        if "orders" in feats["tables"] and "o_custkey" in feats["filter_columns"]:
            suggestions.append("CREATE INDEX ON orders(o_custkey);")
        if "lineitem" in feats["tables"] and "l_orderkey" in feats["filter_columns"]:
            suggestions.append("CREATE INDEX ON lineitem(l_orderkey);")

    # Rule 2: Aggregation on moderately large data
    if feats["has_groupby"] and feats["plan_rows"] > 100:  # Reduced from 1000
        suggestions.append("Consider a materialized view for this aggregation.")
    # 3. Filter indexes
    if feats["n_filter_cols"] >= 2:
    # Add filter-specific index suggestions
        pass

# 4. Large result warning
    if feats["plan_rows"] > 1000:
        suggestions.append("⚠️ Add LIMIT to prevent large result sets")
    # ALWAYS show suggestions if they exist (proactive advice)
    if suggestions:
        print("💡 Optimization suggestion:")
        for s in suggestions:
            print(f"   - {s}")

if __name__ == "__main__":
    main()