FROM python@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS base

WORKDIR /app

# Dependencies and the shared package, so Docker caches this layer
# and doesn't reinstall on every service code change
COPY pyproject.toml ./
COPY shared/ ./shared/
RUN pip install --no-cache-dir .


# One-shot job that `make up` runs once, before the services start
FROM base AS migrate

COPY alembic.ini ./
COPY migrations/ ./migrations/

CMD ["alembic", "upgrade", "head"]


# Last stage, so the service builds in docker-compose.yml get it without naming a target
FROM base

# Which service this image runs - passed in from docker-compose.yml
ARG SERVICE
ENV SERVICE=${SERVICE}

# The one service this image runs
COPY ${SERVICE}/ ./${SERVICE}/

EXPOSE 8000

# Shell form, because expansion of $SERVICE is needed here
CMD exec uvicorn ${SERVICE}.main:app --host 0.0.0.0 --port 8000
