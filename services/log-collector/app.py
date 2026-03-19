from fastapi import FastAPI
from kubernetes import client, config
from datetime import datetime
import psycopg2
import os
import threading
import time

app = FastAPI(title="Log Collector")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "logsdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pod_logs (
            id SERIAL PRIMARY KEY,
            pod_name VARCHAR(255),
            namespace VARCHAR(100),
            log_line TEXT,
            collected_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def collect_logs():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    while True:
        try:
            pods = v1.list_namespaced_pod(namespace="default")
            conn = get_db()
            cur = conn.cursor()
            for pod in pods.items:
                try:
                    logs = v1.read_namespaced_pod_log(
                        name=pod.metadata.name,
                        namespace="default",
                        tail_lines=20
                    )
                    for line in logs.splitlines():
                        cur.execute(
                            "INSERT INTO pod_logs (pod_name, namespace, log_line) VALUES (%s, %s, %s)",
                            (pod.metadata.name, "default", line)
                        )
                except Exception as e:
                    pass
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error collecting logs: {e}")
        time.sleep(30)

@app.on_event("startup")
async def startup():
    init_db()
    thread = threading.Thread(target=collect_logs, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/logs")
def get_logs(limit: int = 100):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT pod_name, log_line, collected_at FROM pod_logs ORDER BY collected_at DESC LIMIT %s",
        (limit,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"pod": r[0], "log": r[1], "time": str(r[2])} for r in rows]

@app.get("/logs/{pod_name}")
def get_pod_logs(pod_name: str, limit: int = 50):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT log_line, collected_at FROM pod_logs WHERE pod_name=%s ORDER BY collected_at DESC LIMIT %s",
        (pod_name, limit)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"log": r[0], "time": str(r[1])} for r in rows]
