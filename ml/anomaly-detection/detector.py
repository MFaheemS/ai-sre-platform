from fastapi import FastAPI
from sklearn.ensemble import IsolationForest
import numpy as np
import requests
import os
import threading
import time
import json
from datetime import datetime

app = FastAPI(title="Anomaly Detector")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")

model = IsolationForest(contamination=0.1, random_state=42)
anomalies = []
is_trained = False

def fetch_metric(query: str) -> list:
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=5
        )
        data = response.json()
        results = data.get("data", {}).get("result", [])
        return [float(r["value"][1]) for r in results if r["value"][1] != "NaN"]
    except Exception as e:
        print(f"Error fetching metric: {e}")
        return []

def collect_and_detect():
    global is_trained, anomalies
    history = []
    while True:
        try:
            cpu = fetch_metric("rate(process_cpu_seconds_total[1m])")
            memory = fetch_metric("process_resident_memory_bytes")
            if not cpu:
                cpu = [0.0]
            if not memory:
                memory = [0.0]
            cpu_val = np.mean(cpu)
            mem_val = np.mean(memory)
            history.append([cpu_val, mem_val])
            if len(history) >= 10:
                X = np.array(history[-50:])
                model.fit(X)
                is_trained = True
                current = np.array([[cpu_val, mem_val]])
                prediction = model.predict(current)[0]
                score = model.score_samples(current)[0]
                if prediction == -1:
                    anomaly = {
                        "timestamp": datetime.now().isoformat(),
                        "cpu": round(cpu_val, 6),
                        "memory_mb": round(mem_val / 1024 / 1024, 2),
                        "anomaly_score": round(score, 4),
                        "severity": "high" if score < -0.2 else "medium"
                    }
                    anomalies.append(anomaly)
                    anomalies = anomalies[-100:]
                    print(f"ANOMALY DETECTED: {anomaly}")
        except Exception as e:
            print(f"Detection error: {e}")
        time.sleep(15)

@app.on_event("startup")
async def startup():
    thread = threading.Thread(target=collect_and_detect, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "healthy", "model_trained": is_trained}

@app.get("/anomalies")
def get_anomalies():
    return {
        "total": len(anomalies),
        "anomalies": anomalies[-20:]
    }

@app.get("/status")
def status():
    cpu = fetch_metric("rate(process_cpu_seconds_total[1m])")
    memory = fetch_metric("process_resident_memory_bytes")
    return {
        "model_trained": is_trained,
        "current_cpu": round(np.mean(cpu), 6) if cpu else 0,
        "current_memory_mb": round(np.mean(memory) / 1024 / 1024, 2) if memory else 0,
        "total_anomalies_detected": len(anomalies)
    }