.PHONY: up bootstrap status dbt-build dbt-test demo-cicd logs down clean

up:
	docker compose up -d
	python3 scripts/bootstrap_lakehouse.py

bootstrap:
	python3 scripts/bootstrap_lakehouse.py

status:
	docker compose ps

dbt-build:
	docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse

dbt-test:
	docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt test --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse

demo-cicd:
	docker exec lakehouse_airflow airflow dags trigger lakehouse_data_cicd_pipeline

down:
	docker compose down

clean:
	docker compose down -v
