# src/web/app.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from flask import Flask, render_template, request, jsonify
import joblib
import pandas as pd
import numpy as np
from src.features import compute_features

app = Flask(__name__)

# Load model and config
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "models", "runtime_predictor.joblib")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "models", "feature_config.joblib")

model = joblib.load(MODEL_PATH)
config = joblib.load(CONFIG_PATH)

def generate_suggestions(feats):
    """Generate all actionable suggestions based on query features"""
    suggestions = []
    
    # 1. Join indexes (for any join)
    if feats["n_joins"] >= 1:
        if "customer" in feats["tables"] and "c_custkey" in feats["filter_columns"]:
            suggestions.append("CREATE INDEX ON customer(c_custkey);")
        if "orders" in feats["tables"] and "o_custkey" in feats["filter_columns"]:
            suggestions.append("CREATE INDEX ON orders(o_custkey);")
        if "lineitem" in feats["tables"] and "l_orderkey" in feats["filter_columns"]:
            suggestions.append("CREATE INDEX ON lineitem(l_orderkey);")
    
    # 2. Aggregation optimization
    if feats["has_groupby"] and feats["plan_rows"] > 100:
        suggestions.append("Consider a materialized view for this aggregation.")
    
    # 3. Large result warning
    if feats["plan_rows"] > 1000:
        suggestions.append("⚠️ Add LIMIT clause to prevent large result sets")
    
    return suggestions

@app.route("/")
def index():
    return render_template("index.html")
@app.route("/optimize", methods=["POST"])
def optimize():
    sql = request.json.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Please enter a SQL query"}), 400
    
    try:
        # Get original prediction
        feats_orig = compute_features(sql)
        feature_cols = config["feature_cols"]
        X_orig = pd.DataFrame([[
            feats_orig["n_joins"],
            feats_orig["has_groupby"],
            feats_orig["n_filter_cols"],
            feats_orig["plan_rows"]
        ]], columns=feature_cols)
        original_runtime = float(model.predict(X_orig)[0])
        
        # Rewrite query
        from src.optimizer.rewriter import rewrite_query
        rewrite_result = rewrite_query(sql)
        optimized_sql = rewrite_result["optimized"]
        
        # Get optimized prediction
        feats_opt = compute_features(optimized_sql)
        X_opt = pd.DataFrame([[
            feats_opt["n_joins"],
            feats_opt["has_groupby"],
            feats_opt["n_filter_cols"],
            feats_opt["plan_rows"]
        ]], columns=feature_cols)
        optimized_runtime = float(model.predict(X_opt)[0])
        
        speedup = round(original_runtime / optimized_runtime, 1) if optimized_runtime > 0 else 1.0
        
        # Generate suggestions for original query
        suggestions = []
        if feats_orig["n_joins"] >= 1:
            if "customer" in feats_orig["tables"] and "c_custkey" in feats_orig["filter_columns"]:
                suggestions.append("CREATE INDEX ON customer(c_custkey);")
            if "orders" in feats_orig["tables"] and "o_custkey" in feats_orig["filter_columns"]:
                suggestions.append("CREATE INDEX ON orders(o_custkey);")
            if "lineitem" in feats_orig["tables"] and "l_orderkey" in feats_orig["filter_columns"]:
                suggestions.append("CREATE INDEX ON lineitem(l_orderkey);")
        
        if feats_orig["has_groupby"] and feats_orig["plan_rows"] > 100:
            suggestions.append("Consider a materialized view for this aggregation.")
        
        return jsonify({
            "original_query": sql,
            "original_runtime_ms": round(original_runtime, 2),
            "optimized_query": optimized_sql,
            "optimized_runtime_ms": round(optimized_runtime, 2),
            "speedup_factor": speedup,
            "improvements": rewrite_result["improvements"],
            "suggestions": suggestions
        })
    
    except Exception as e:
        return jsonify({"error": f"Optimization failed: {str(e)}"}), 500
    
@app.route("/predict", methods=["POST"])
def predict():
    sql = request.json.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Please enter a SQL query"}), 400
    
    try:
        # Compute features
        feats = compute_features(sql)
        
        # Build feature vector (4 structural features)
        feature_cols = config["feature_cols"]
        X = pd.DataFrame([[
            feats["n_joins"],
            feats["has_groupby"],
            feats["n_filter_cols"],
            feats["plan_rows"]
        ]], columns=feature_cols)
        
        # Predict raw runtime
        pred_ms = float(model.predict(X)[0])
        
        # Generate suggestions
        suggestions = generate_suggestions(feats)
        
        # Determine warning level
        if pred_ms > 10.0:
            warning_level = "high"
            warning_text = "⚠️ SLOW QUERY"
        elif pred_ms > 5.0:
            warning_level = "medium"
            warning_text = "🔶 Medium"
        else:
            warning_level = "low"
            warning_text = "✅ Fast"
        
        return jsonify({
            "predicted_runtime_ms": round(pred_ms, 2),
            "warning_level": warning_level,
            "warning_text": warning_text,
            "suggestions": suggestions,
            "features": {
                "n_joins": feats["n_joins"],
                "has_groupby": bool(feats["has_groupby"]),
                "n_filter_cols": feats["n_filter_cols"],
                "plan_rows": feats["plan_rows"]
            }
        })
    
    except Exception as e:
        return jsonify({"error": f"Prediction failed: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)