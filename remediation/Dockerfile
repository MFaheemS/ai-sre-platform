FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY remediation.py .
EXPOSE 8004
CMD ["uvicorn", "remediation:app", "--host", "0.0.0.0", "--port", "8004"]