include .env
export

COMPOSE = docker compose -f infra/docker-compose.yml --project-directory .
# Small heap of its own, because the broker's KAFKA_HEAP_OPTS would give this JVM 512m too
KAFKA_TOPICS = $(COMPOSE) exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
# Empty until orders and inventory emit real events, then their topics are listed here
TOPICS =

# Defaults for make test, override with e.g. make test REQUESTS=200 LIMIT=50
REQUESTS ?= 100
LIMIT ?= 1000
IMAGES ?= 20

.PHONY: up down obs seed health lint test test-images

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
	$(COMPOSE) --profile obs down

obs: up
	$(COMPOSE) --profile obs up -d --wait flower

# Replaces the whole catalogue, so it always ends with the same products
seed:
	$(COMPOSE) exec -T catalog python -m catalog.seed

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

# REQUESTS parallel requests to GET /catalog/products?limit=LIMIT, with server and client times
test:
	python3 bench/parallel_products.py --requests $(REQUESTS) --limit $(LIMIT)

# IMAGES parallel uploads to POST /catalog/products/{id}/image, timed until catalog-worker has
# processed them all, then every image is deleted
test-images:
	python3 bench/parallel_images.py --images $(IMAGES)

