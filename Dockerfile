FROM apache/airflow:2.10.5
COPY requirements.txt .
COPY constraints.txt .
RUN pip install -r requirements.txt

