# QueryMind: AI Co-Pilot for PostgreSQL

> **Predict query runtime before execution** and get actionable optimization suggestions.

![QueryMind Demo](/test_web-app1.png)

## 🎯 The Problem
Every developer has faced this:
- A query takes 10ms in dev but 2s in production
- `EXPLAIN` shows what happened, not what will happen  
- Missing indexes cause 10-100x slowdowns

**Existing tools are reactive. QueryMind is proactive.**

## 💡 Our Solution
An AI system that:
- **Predicts runtime** with 91% accuracy on TPC-H benchmark
- **Suggests exact fixes**: `CREATE INDEX ON orders(o_custkey);`
- **Works before execution** — prevent slow queries, don't debug them

## 🚀 Quick Start
```bash
git clone https://github.com/achrafide/querymind
cd querymind
./launch.sh
python src/advisor/predict.py "SELECT c.c_name FROM customer c, orders o WHERE c.c_custkey = o.o_custkey;"