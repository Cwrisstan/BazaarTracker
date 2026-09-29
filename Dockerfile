FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY tools/ ./tools/
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "src/ingestion/run_collector.py"]
