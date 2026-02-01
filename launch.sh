#!/bin/bash
# launch.sh - One-command setup for QueryMind

set -e  # Exit on error

echo "🚀 Launching QueryMind Dev Environment..."

# 1. Start PostgreSQL (with pg_stat_statements preloaded)
echo "🐳 Starting PostgreSQL..."
docker-compose up -d
sleep 12  # Wait for full startup

# 2. Create pg_stat_statements extension (idempotent)
echo "🔌 Enabling pg_stat_statements extension..."
HOST=$(awk '/nameserver/{print $2}' /etc/resolv.conf)
PGPASS="querymind123" psql -h "$HOST" -p 5432 -U postgres -d tpch -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" 2>/dev/null || true

# 3. Load TPC-H data
echo "🗃️  Loading TPC-H sample data..."
cd data/sample_tpch
python load_data.py
cd ../..

# 4. Log all 22 TPC-H queries
echo "📝 Logging query runtimes..."
python scripts/log_queries.py

echo ""
echo "🎉 QueryMind is READY!"
echo "   - Dataset: data/query_log.csv"
echo "   - Next: explore with scripts/explore.py or train model"