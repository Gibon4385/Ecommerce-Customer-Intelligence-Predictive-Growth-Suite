FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir Faker kafka-python-ng orjson python-dotenv

COPY scripts/run_gap_filler.py scripts/run_kafka_producer.py ./scripts/
RUN mkdir -p data

ENTRYPOINT ["python", "scripts/run_kafka_producer.py"]
CMD ["--delay", "1.0"]
