FROM python:3.11-slim

WORKDIR /app

COPY examples/realtime-inference/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/realtime-inference/ ./examples/realtime-inference/

ENV MODEL_SERVER_PORT=8080
EXPOSE 8080

CMD ["python", "examples/realtime-inference/model_server.py"]
