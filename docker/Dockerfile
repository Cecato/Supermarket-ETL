FROM bitnami/spark:latest

USER root

# Instale o curl (utilitário Bitnami)
RUN install_packages curl

WORKDIR /app
COPY . /app

# Instale as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Baixe o driver JDBC do PostgreSQL
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER 1001

CMD ["spark-submit", "main.py"]
