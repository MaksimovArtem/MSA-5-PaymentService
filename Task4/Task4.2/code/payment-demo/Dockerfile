FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY proto/gateway.proto /proto/gateway.proto

RUN pip install --no-cache-dir grpcio==1.68.1 grpcio-tools==1.68.1 protobuf==5.28.3 && \
    python -m grpc_tools.protoc -I /proto --python_out /app --grpc_python_out /app /proto/gateway.proto

COPY payment-demo/demo.py /app/demo.py

ENTRYPOINT ["python", "/app/demo.py"]

