include .env
export

COMPOSE = docker compose -f infra/docker-compose.yml --project-directory .

.PHONY: up down health

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

health:
	@curl -fsS localhost:$(CATALOG_PORT)/health && echo
	@curl -fsS localhost:$(ORDERS_PORT)/health && echo
	@curl -fsS localhost:$(INVENTORY_PORT)/health && echo
	@curl -fsS localhost:$(PAYMENTS_PORT)/health && echo
	@curl -fsS localhost:$(BILLING_PORT)/health && echo
