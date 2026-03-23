from fastapi import FastAPI
from kubernetes import client, config
import requests
import os
import threading
import time
from datetime import datetime

app = FastAPI(title="Remediation Engine")

ANOMALY_DETECTOR_URL = os.getenv("ANOMALY_DETECTOR_URL", "http://anomaly-detector:8002")

remediation_history = []

def get_k8s_client():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    return client.AppsV1Api(), client.CoreV1Api()

def restart_pod(pod_name: str, namespace: str = "default") -> dict:
    try:
        _, v1 = get_k8s_client()
        v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
        return {"action": "restart_pod", "target": pod_name, "status": "success"}
    except Exception as e:
        return {"action": "restart_pod", "target": pod_name, "status": "failed", "error": str(e)}

def scale_deployment(name: str, replicas: int, namespace: str = "default") -> dict:
    try:
        apps_v1, _ = get_k8s_client()
        body = {"spec": {"replicas": replicas}}
        apps_v1.patch_namespaced_deployment_scale(name=name, namespace=namespace, body=body)
        return {"action": "scale_deployment", "target": name, "replicas": replicas, "status": "success"}
    except Exception as e:
        return {"action": "scale_deployment", "target": name, "status": "failed", "error": str(e)}

def decide_remediation(anomaly: dict) -> dict:
    severity = anomaly.get("severity", "medium")
    cpu = anomaly.get("cpu", 0)
    if severity == "high" and cpu > 0.5:
        return {"type": "scale_up", "deployment": "cpu-spike-service", "replicas": 3}
    elif severity == "high":
        return {"type": "restart", "deployment": "cpu-spike-service"}
    elif severity == "medium":
        return {"type": "scale_up", "deployment": "cpu-spike-service", "replicas": 2}
    return {"type": "none"}

def execute_remediation(action: dict) -> dict:
    action_type = action.get("type")
    if action_type == "scale_up":
        return scale_deployment(action["deployment"], action["replicas"])
    elif action_type == "restart":
        apps_v1, _ = get_k8s_client()
        pods = client.CoreV1Api().list_namespaced_pod(
            namespace="default",
            label_selector=f"app={action['deployment']}"
        )
        if pods.items:
            return restart_pod(pods.items[0].metadata.name)
    return {"action": "none", "status": "no_action_needed"}

def auto_remediate():
    while True:
        try:
            response = requests.get(f"{ANOMALY_DETECTOR_URL}/anomalies", timeout=5)
            data = response.json()
            anomalies = data.get("anomalies", [])
            if anomalies:
                latest = anomalies[-1]
                already_handled = any(
                    r.get("anomaly_timestamp") == latest.get("timestamp")
                    for r in remediation_history
                )
                if not already_handled:
                    action = decide_remediation(latest)
                    if action["type"] != "none":
                        result = execute_remediation(action)
                        record = {
                            "timestamp": datetime.now().isoformat(),
                            "anomaly_timestamp": latest.get("timestamp"),
                            "anomaly_severity": latest.get("severity"),
                            "action_taken": action,
                            "result": result
                        }
                        remediation_history.append(record)
                        remediation_history_trimmed = remediation_history[-50:]
                        print(f"REMEDIATION: {record}")
        except Exception as e:
            print(f"Auto remediation error: {e}")
        time.sleep(20)

@app.on_event("startup")
async def startup():
    thread = threading.Thread(target=auto_remediate, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/history")
def get_history():
    return {
        "total": len(remediation_history),
        "history": remediation_history[-20:]
    }

@app.post("/remediate/scale/{deployment}/{replicas}")
def manual_scale(deployment: str, replicas: int):
    result = scale_deployment(deployment, replicas)
    record = {
        "timestamp": datetime.now().isoformat(),
        "trigger": "manual",
        "action_taken": {"type": "scale", "deployment": deployment, "replicas": replicas},
        "result": result
    }
    remediation_history.append(record)
    return result

@app.post("/remediate/restart/{deployment}")
def manual_restart(deployment: str):
    apps_v1, _ = get_k8s_client()
    pods = client.CoreV1Api().list_namespaced_pod(
        namespace="default",
        label_selector=f"app={deployment}"
    )
    if not pods.items:
        return {"error": f"No pods found for {deployment}"}
    result = restart_pod(pods.items[0].metadata.name)
    record = {
        "timestamp": datetime.now().isoformat(),
        "trigger": "manual",
        "action_taken": {"type": "restart", "deployment": deployment},
        "result": result
    }
    remediation_history.append(record)
    return result