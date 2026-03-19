from fastapi import FastAPI
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import threading
import random
import time
import math

app = FastAPI(title="CPU Spike Service")

REQUEST_COUNT = Counter("app_requests_total", "Total requests", ["endpoint"])
REQUEST_LATENCY = Histogram("app_request_latency_seconds", "Request latency")

def cpu_spike(duration: float):
    end = time.time() + duration
    while time.time() < end:
        math.factorial(10000)

@app.get("/")
def root():
    REQUEST_COUNT.labels(endpoint="/").inc()
    return {"status": "running", "service": "cpu-spike-service"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/spike")
def trigger_spike():
    REQUEST_COUNT.labels(endpoint="/spike").inc()
    duration = random.uniform(2, 5)
    thread = threading.Thread(target=cpu_spike, args=(duration,))
    thread.start()
    return {"status": "spike triggered", "duration_seconds": round(duration, 2)}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.on_event("startup")
async def auto_spike():
    def random_spikes():
        while True:
            time.sleep(random.randint(10, 30))
            cpu_spike(random.uniform(1, 4))
    thread = threading.Thread(target=random_spikes, daemon=True)
    thread.start()
