# Credentials live in .env, which git ignores. Make creates it on first run
# from .env.example with random passwords, then exports it to every recipe.
-include .env
export

COMPOSE := docker compose
RUN := $(COMPOSE) run --rm consumer python -m

.PHONY: help up register generate reconcile status violations history demo test integration lint down k8s-up k8s-demo k8s-down

help:
	@echo "make demo         start the stack, register connectors, generate traffic, reconcile"
	@echo "make status       connector and task status"
	@echo "make violations   rule violations the consumer has flagged"
	@echo "make history      status history for the 10 most recent transactions"
	@echo "make test         unit tests"
	@echo "make integration  end-to-end test against the running stack"
	@echo "make down         stop everything and delete volumes"
	@echo "make k8s-up       run it all on a local Kubernetes cluster (k3d, Strimzi, Terraform, Helm)"
	@echo "make k8s-demo     generate traffic and reconcile on that cluster"
	@echo "make k8s-down     delete the local cluster"

up:
	$(COMPOSE) up -d --build --wait mysql kafka postgres connect consumer

register:
	./scripts/register_connectors.sh

generate:
	$(RUN) cdc.simulate --transactions 500 --anomalies 5

reconcile:
	$(RUN) cdc.reconcile --today --wait-seconds 120

status:
	@curl -s "localhost:8083/connectors?expand=status" | python3 -m json.tool

violations:
	$(COMPOSE) exec postgres psql -U "$(PG_USER)" -d warehouse -c \
	  "SELECT rule, COUNT(*) FROM cdc.transition_violations GROUP BY rule ORDER BY rule"

history:
	$(COMPOSE) exec postgres psql -U "$(PG_USER)" -d warehouse -c \
	  "SELECT transaction_id, op, from_status, to_status, source_ts FROM cdc.transaction_status_history \
	   WHERE transaction_id IN (SELECT id FROM public.transactions ORDER BY id DESC LIMIT 10) \
	   ORDER BY transaction_id, source_ts"

demo: up register generate reconcile violations

test:
	pytest -m "not integration" -q

integration:
	RUN_INTEGRATION=1 MYSQL_HOST=127.0.0.1 MYSQL_PORT=3307 PG_HOST=127.0.0.1 PG_PORT=5433 \
	  pytest -m integration -v

lint:
	ruff check . && ruff format --check .

down:
	$(COMPOSE) down -v

k8s-up:
	./deploy/local/up.sh

k8s-demo:
	./deploy/local/demo.sh

k8s-down:
	./deploy/local/down.sh

.env:
	./scripts/init_env.sh
