FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY feature_to_minio.py .

CMD ["python", "feature_to_minio.py"]
