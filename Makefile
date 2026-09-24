include .env
export

COMPOSE = docker compose -f infra/docker-compose.yml --project-directory .

.PHONY: up down health migrate db

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

health:
	@curl -fsS localhost:$(CATALOG_PORT)/health && echo
	@curl -sS localhost:$(CATALOG_PORT)/ready && echo
	@curl -fsS localhost:$(ORDERS_PORT)/health && echo
	@curl -sS localhost:$(ORDERS_PORT)/ready && echo
	@curl -fsS localhost:$(INVENTORY_PORT)/health && echo
	@curl -sS localhost:$(INVENTORY_PORT)/ready && echo
	@curl -fsS localhost:$(PAYMENTS_PORT)/health && echo
	@curl -sS localhost:$(PAYMENTS_PORT)/ready && echo
	@curl -fsS localhost:$(BILLING_PORT)/health && echo
	@curl -sS localhost:$(BILLING_PORT)/ready && echo

migrate:
	$(COMPOSE) run --rm --build migrate

db:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
