FROM apache/airflow:2.10.5-python3.12

# Keep dbt's dependencies separate from the pinned Airflow image environment.
USER root
RUN mkdir -p /opt/pipeline /opt/airflow/warehouse \
    && chown -R airflow:root /opt/pipeline /opt/airflow/warehouse \
    && chmod -R g+rwX /opt/pipeline /opt/airflow/warehouse
USER airflow

# Only this dependency manifest is copied; code/data/credentials are runtime mounts.
COPY --chown=airflow:root requirements.txt /opt/pipeline/requirements.txt
RUN python -m venv /opt/pipeline/venv \
    && PIP_USER=false /opt/pipeline/venv/bin/pip install --no-cache-dir \
        -r /opt/pipeline/requirements.txt \
    && /opt/pipeline/venv/bin/pip check

# Cache extensions in /home/airflow/.duckdb, outside the persistent AIRFLOW_HOME.
# Image building needs network access; task execution need not download extensions.
RUN /opt/pipeline/venv/bin/python -c \
    "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('INSTALL aws'); con.execute('LOAD httpfs'); con.execute('LOAD aws'); con.close()" \
    && chmod -R a+rX /home/airflow/.duckdb
