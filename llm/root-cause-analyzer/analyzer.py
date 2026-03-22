from fastapi import FastAPI
from openai import OpenAI
import requests
import os
from datetime import datetime

app = FastAPI(title="Root Cause Analyzer")

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url="https://api.groq.com/openai/v1"
)

LOG_COLLECTOR_URL = os.getenv("LOG_COLLECTOR_URL", "http://log-collector:8001")
ANOMALY_DETECTOR_URL = os.getenv("ANOMALY_DETECTOR_URL", "http://anomaly-detector:8002")

analyses = []

def fetch_logs(pod_name: str = None) -> list:
    try:
        url = f"{LOG_COLLECTOR_URL}/logs"
        if pod_name:
            url = f"{LOG_COLLECTOR_URL}/logs/{pod_name}"
        response = requests.get(url, timeout=5)
        return response.json()
    except Exception as e:
        print(f"Error fetching logs: {e}")
        return []

def fetch_anomalies() -> dict:
    try:
        response = requests.get(f"{ANOMALY_DETECTOR_URL}/anomalies", timeout=5)
        return response.json()
    except Exception as e:
        print(f"Error fetching anomalies: {e}")
        return {}

def analyze_with_llm(logs: list, anomalies: dict) -> dict:
    log_text = "\n".join([
        f"[{l.get('pod')}] {l.get('log')}"
        for l in logs[:30]
    ])
    anomaly_text = str(anomalies.get("anomalies", [])[-5:])

    prompt = f"""You are an expert SRE analyzing a Kubernetes infrastructure issue.

RECENT ANOMALIES DETECTED:
{anomaly_text}

RECENT POD LOGS:
{log_text}

Please analyze this data and provide:
1. ROOT CAUSE: What is causing the issue?
2. SEVERITY: How serious is this? (low/medium/high/critical)
3. AFFECTED COMPONENTS: Which services are impacted?
4. RECOMMENDED FIX: What action should be taken?
5. PREVENTION: How to prevent this in the future?

Be concise and specific."""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )

    return {
        "timestamp": datetime.now().isoformat(),
        "analysis": response.choices[0].message.content,
        "log_count": len(logs),
        "anomaly_count": anomalies.get("total", 0)
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/analyze")
def analyze():
    logs = fetch_logs()
    anomalies = fetch_anomalies()
    if not logs and not anomalies.get("anomalies"):
        return {"error": "No data available for analysis"}
    result = analyze_with_llm(logs, anomalies)
    analyses.append(result)
    return result

@app.get("/analyses")
def get_analyses():
    return {
        "total": len(analyses),
        "analyses": analyses[-10:]
    }

@app.post("/analyze/pod/{pod_name}")
def analyze_pod(pod_name: str):
    logs = fetch_logs(pod_name)
    anomalies = fetch_anomalies()
    if not logs:
        return {"error": f"No logs found for pod {pod_name}"}
    result = analyze_with_llm(logs, anomalies)
    result["pod_name"] = pod_name
    analyses.append(result)
    return result