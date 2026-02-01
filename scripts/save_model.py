# scripts/save_model.py
import joblib
import os

# Create models dir
os.makedirs("models", exist_ok=True)

# Save model (dummy for now — replace with real one later)
from sklearn.ensemble import RandomForestRegressor
import numpy as np

# Dummy model (you'll replace this with your trained one)
model = RandomForestRegressor()
model.fit(np.array([[1,2,3,4,5]]), np.array([10.0]))

joblib.dump(model, "models/runtime_predictor.joblib")

# Save feature config
feature_config = {
    "table_rows": {
        "customer": 150,
        "orders": 1500,
        "lineitem": 6000,
        "part": 200,
        "supplier": 50,
        "nation": 25,
        "region": 5
    },
    "feature_cols": ["log_estimated_rows", "n_tables", "n_joins", "n_filters", "has_groupby"]
}
joblib.dump(feature_config, "models/feature_config.joblib")

print("✅ Model and config saved to models/")