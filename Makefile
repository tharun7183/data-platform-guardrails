.PHONY: up down logs init run psql

up:
	docker compose -f docker/docker-compose.yml up -d --build

down:
	docker compose -f docker/docker-compose.yml down -v

logs:
	docker compose -f docker/docker-compose.yml logs -f --tail=200

init:
	# initialize airflow DB + create admin user
	docker exec -it dp_airflow_sched airflow db init
	docker exec -it dp_airflow_sched airflow users create \
	  --username admin --firstname admin --lastname admin \
	  --role Admin --email admin@example.com --password admin || true
	# add Postgres + Spark connections
	docker exec -it dp_airflow_sched airflow connections add dp_postgres \
	  --conn-uri 'postgresql://dp:dp@postgres:5432/dp' || true
	docker exec -it dp_airflow_sched airflow connections add spark_default \
	  --conn-type spark --conn-host spark --conn-port 7077 || true

run:
	docker exec -it dp_airflow_sched airflow dags trigger data_platform_guardrails_pipeline

psql:
	docker exec -it dp_postgres psql -U dp -d dp
