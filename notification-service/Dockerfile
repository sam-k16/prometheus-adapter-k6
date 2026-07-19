FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home \
    --uid 10001 \
    --shell /usr/sbin/nologin \
    appuser

COPY main.py .

USER 10001

EXPOSE 8002

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
#CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--exporter_otlp_endpoint", "http://otel-collector.monitoring:4317", "--service_name", "notification-service", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
