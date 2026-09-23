FROM python@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea

# Which service this image runs - passed in from docker-compose.yml
ARG SERVICE
ENV SERVICE=${SERVICE}

WORKDIR /app

# Dependencies and the shared package, so Docker caches this layer
# and doesn't reinstall on every service code change
COPY pyproject.toml ./
COPY shared/ ./shared/
RUN pip install --no-cache-dir .

# The one service this image runs
COPY ${SERVICE}/ ./${SERVICE}/

EXPOSE 8000

# Shell form, because expansion of $SERVICE is needed here
CMD uvicorn ${SERVICE}.main:app --host 0.0.0.0 --port 8000 --log-level ${LOG_LEVEL:-info}
