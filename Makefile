include .env
export

COMPOSE = docker compose -f infra/docker-compose.yml --project-directory .
# Small heap of its own, because the broker's KAFKA_HEAP_OPTS would give this JVM 512m too
KAFKA_TOPICS = $(COMPOSE) exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
# Empty until orders and inventory emit real events, then their topics are listed here
TOPICS =

.PHONY: up down health lint

# Services start last, so the schema and the topics already exist when their consumers start.
# An existing topic keeps its partition count, so a changed KAFKA_PARTITIONS fails here loudly
up:
	$(COMPOSE) build
	$(COMPOSE) up -d --wait postgres kafka
	$(COMPOSE) run --rm --build migrate
	@for topic in $(TOPICS); do \
		$(KAFKA_TOPICS) --create --if-not-exists --topic $$topic \
			--partitions $(KAFKA_PARTITIONS) --replication-factor 1 || exit 1; \
		$(KAFKA_TOPICS) --describe --topic $$topic \
			| grep -q "PartitionCount: $(KAFKA_PARTITIONS)[[:space:]]" || { \
			echo "$$topic exists with a partition count other than KAFKA_PARTITIONS=$(KAFKA_PARTITIONS)," \
				"recreate it with a clean stack (docker compose down -v)"; \
			exit 1; }; \
	done
	$(COMPOSE) up -d --wait

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

