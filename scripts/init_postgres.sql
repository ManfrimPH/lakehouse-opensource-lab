-- Criação dos bancos de metadados para Airflow e Superset
CREATE DATABASE airflow_db;
CREATE DATABASE superset_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO lakehouse;
GRANT ALL PRIVILEGES ON DATABASE superset_db TO lakehouse;
