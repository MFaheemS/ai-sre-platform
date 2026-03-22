FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY analyzer.py .
EXPOSE 8003
CMD ["uvicorn", "analyzer:app", "--host", "0.0.0.0", "--port", "8003"]