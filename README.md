# QueryMind 🧠

An AI-assisted query optimizer that **learns from your workload** to suggest faster SQL plans, better indexes, and schema improvements.

> "Not a replacement for the optimizer — an AI co-pilot for your database."

## 🚀 Quick Start

```bash
# 1. Start PostgreSQL with TPC-H
docker-compose up -d

# 2. Load sample data
python src/db_connector.py --load-data

# 3. Run query logging
python src/query_logger.py

# 4. Explore in notebook
jupyter notebook notebooks/01_explore_query_log.ipynb