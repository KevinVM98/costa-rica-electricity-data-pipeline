
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
