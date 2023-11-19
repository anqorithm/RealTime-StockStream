FROM python:3.11-slim

WORKDIR /app

COPY . . 

RUN pip install --no-cache-dir kafka-python

CMD ["python", "producer.py"]