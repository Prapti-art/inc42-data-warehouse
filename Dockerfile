# Inc42 Data Warehouse — Full Pipeline
# Contains: Airflow + PySpark + dbt + Python ingestion scripts

FROM python:3.11-slim

# ── Install Java (required for PySpark) ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Install Python packages ──
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── Set up Airflow ──
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
ENV AIRFLOW__WEBSERVER__SECRET_KEY=inc42-data-warehouse-secret-key

# ── Copy project files ──
WORKDIR /opt/inc42-data-warehouse

COPY scripts/ scripts/
COPY spark/ spark/
COPY airflow/dags/ /opt/airflow/dags/
COPY ingestion/ ingestion/
COPY dbt/ dbt/
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# ── Create Airflow directories ──
RUN mkdir -p /opt/airflow/logs /opt/airflow/plugins

# ── dbt project will be mounted as volume ──
# ── Service account will be mounted as volume ──

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
