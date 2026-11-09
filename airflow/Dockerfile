
# ==========================================
# ARESEP - AIRFLOW PERSONALIZADO
# ==========================================

# Imagen oficial de Apache Airflow
FROM apache/airflow:3.3.2

# Dependencias necesarias para nuestro DAG
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "requests==2.34.2" \
    "psycopg[binary]==3.3.5" \
    "pendulum==3.2.0"

# Entorno independiente para las dependencias de dbt.
COPY dbt/requirements.txt /tmp/aresep-dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/python -m pip install --no-cache-dir -r /tmp/aresep-dbt-requirements.txt

COPY dbt/dbt_project.yml dbt/profiles.yml /opt/airflow/dbt/
COPY dbt/models/ /opt/airflow/dbt/models/
COPY dbt/tests/ /opt/airflow/dbt/tests/
