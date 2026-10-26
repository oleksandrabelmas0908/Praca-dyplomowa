include .env
export

OBS ?= true
APP_FILES = -f infra/docker-compose.yml
ALL_FILES = $(APP_FILES) -f infra/docker-compose.obs.yml
COMPOSE = docker compose $(if $(filter true,$(OBS)),$(ALL_FILES),$(APP_FILES)) --project-directory .
# Small heap of its own, because the broker's KAFKA_HEAP_OPTS would give this JVM 512m too
KAFKA_TOPICS = $(COMPOSE) exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
# Every topic a service publishes to
TOPICS = order.created inventory.reserved inventory.rejected payment.completed
# Services with a Kafka consumer or producer, stopped while the topics are recreated
KAFKA_SERVICES = orders inventory payments billing

# Creates any missing topic with KAFKA_PARTITIONS and fails on an existing one with another count
define CREATE_TOPICS
	@for topic in $(TOPICS); do \
		$(KAFKA_TOPICS) --create --if-not-exists --topic $$topic \
			--partitions $(KAFKA_PARTITIONS) --replication-factor 1 || exit 1; \
		$(KAFKA_TOPICS) --describe --topic $$topic \
			| grep -q "PartitionCount: $(KAFKA_PARTITIONS)[[:space:]]" || { \
			echo "$$topic exists with a partition count other than KAFKA_PARTITIONS=$(KAFKA_PARTITIONS)," \
				"recreate it with make reset-topics"; \
			exit 1; }; \
	done
endef

# Defaults for make test, override with e.g. make test REQUESTS=200 LIMIT=50
REQUESTS ?= 100
LIMIT ?= 1000
ORDERS ?= 20
LINES ?= 3

.PHONY: up down obs seed reset reset-topics health lint test test-invoices

# OBS=false leaves out infra/docker-compose.obs.yml and removes its containers if they are running.
# Services start last, so the schema and the topics already exist when their consumers start.
# An existing topic keeps its partition count, so a changed KAFKA_PARTITIONS fails here loudly
# until make reset-topics recreates it
up:
	$(COMPOSE) build
	$(COMPOSE) up -d --wait postgres kafka
	$(COMPOSE) run --rm --build migrate
	$(CREATE_TOPICS)
	$(COMPOSE) up -d --wait --remove-orphans

# Both files whatever OBS is, so monitoring containers and volumes go too
down:
	docker compose $(ALL_FILES) --project-directory . down -v

# make up with monitoring, whatever OBS is in .env
obs:
	$(MAKE) up OBS=true

# Replaces the whole catalogue, so it always ends with the same products
seed:
	$(COMPOSE) exec -T catalog python -m catalog.seed

# Partition counts cannot change in place, so every topic is deleted and created again with the
# current KAFKA_PARTITIONS. Events still on the topics are lost. Ends like make up, so the other
# .env changes that a failed make up left unapplied take effect too
reset-topics:
	$(COMPOSE) stop $(KAFKA_SERVICES)
	@for topic in $(TOPICS); do $(KAFKA_TOPICS) --delete --if-exists --topic $$topic || exit 1; done
	@while $(KAFKA_TOPICS) --list | grep -qxF $(addprefix -e ,$(TOPICS)); do sleep 1; done
	$(CREATE_TOPICS)
	$(COMPOSE) up -d --wait --remove-orphans

# Before every measured run, after make up: topics with the current KAFKA_PARTITIONS, stock
# reseeded, and reservations emptied so they add up to stock_reserved again
reset: reset-topics seed
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "TRUNCATE reservations"

health:
	@curl -fsS localhost:$(CATALOG_PORT)/health && echo
	@curl -sS localhost:$(CATALOG_PORT)/ready && echo
	@curl -fsS localhost:$(ORDERS_PORT)/health && echo
	@curl -sS localhost:$(ORDERS_PORT)/ready && echo
	@port=$$($(COMPOSE) port inventory 8000 | sed 's/.*://'); \
		curl -fsS localhost:$$port/health && echo && curl -sS localhost:$$port/ready && echo
	@port=$$($(COMPOSE) port payments 8000 | sed 's/.*://'); \
		curl -fsS localhost:$$port/health && echo && curl -sS localhost:$$port/ready && echo
	@curl -fsS localhost:$(BILLING_PORT)/health && echo
	@curl -sS localhost:$(BILLING_PORT)/ready && echo

# REQUESTS parallel requests to GET /catalog/products?limit=LIMIT, with server and client times
test:
	python3 bench/parallel_products.py --requests $(REQUESTS) --limit $(LIMIT)

# ORDERS parallel orders of LINES lines to POST /orders, timed until billing-worker has generated
# every invoice. Uses up one unit of stock per line, make seed restores it
test-invoices:
	python3 bench/parallel_invoices.py --orders $(ORDERS) --lines $(LINES)
