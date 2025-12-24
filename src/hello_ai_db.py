# hello_ai_db.py
import psycopg2
import socket

def get_docker_host():
    """Get Windows host IP from WSL2"""
    try:
        with open("/etc/resolv.conf") as f:
            for line in f:
                if line.startswith("nameserver"):
                    return line.split()[1]
    except:
        pass
    return "localhost"  # fallback

print("🚀 Connecting to AI-Native DB...")
host = get_docker_host()
print(f"📡 Using host: {host}")

try:
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="tpch",
        user="postgres",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    cur.execute("SELECT 'Hello from Docker PostgreSQL!' AS msg;")
    print("✅ SUCCESS:", cur.fetchone()[0])
    cur.close()
    conn.close()
except Exception as e:
    print("❌ FAILED:", e)
    print("💡 Tip: Make sure Docker Desktop is running and container is up.")