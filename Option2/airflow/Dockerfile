FROM puckel/docker-airflow
ENV AIRFLOW_HOME=/usr/local/airflow
WORKDIR $AIRFLOW_HOME
USER root
RUN pip install pandas
RUN pip install requests
RUN pip install SQLAlchemy
RUN pip install psycopg2-binary
RUN pip install sqlalchemy-utils