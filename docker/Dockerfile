FROM puckel/docker-airflow:1.10.6
RUN pip install --user psycopg2-binary
RUN pip install --user boto3
ENV AIRFLOW_HOME=/usr/local/airflow
COPY airflow.cfg /usr/local/airflow/airflow.cfg