FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir dash plotly pandas cassandra-driver openpyxl

EXPOSE 8050

CMD ["python", "./dashboard.py"]