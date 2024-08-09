FROM apache/airflow:2.9.3

USER root
RUN apt update && apt install -y
RUN apt install -y --no-install-recommends vim

RUN apt install -y openjdk-17-jdk-headless
RUN apt-get install -y procps

COPY ./infra/scala-2.12.10 /usr/local/scala
COPY ./infra/hadoop-3.3.6 /usr/local/hadoop
COPY ./infra/isarn /usr/local/isarn
COPY ./infra/apache-hive-3.1.3-bin /usr/local/hive
COPY ./infra/spark-3.5.1-bin-hadoop3 /usr/local/spark
COPY ./infra/dockerfiles/spark-airflow/entrypoint.sh /usr/local/entrypoint.sh

# Add super user
RUN useradd -m hdoop
RUN usermod -aG sudo hdoop
RUN echo "hdoop:hdoop" | chpasswd

RUN chmod +x /usr/local/entrypoint.sh

USER airflow
COPY ./requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt


COPY ./infra/dockerfiles/spark-airflow/main.py .

ENTRYPOINT [ "/usr/local/entrypoint.sh" ]

