from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    service_name: str
    log_level: LogLevel = "INFO"
    env: Literal["local", "server"] = "local"

    postgres_host: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    db_pool_size: int
    db_max_overflow: int
    db_driver: Literal["async", "sync"] = "async"
    threadpool_size: int = 40

    cache_enabled: bool = False
    cache_ttl_seconds: int = 60
    redis_cache_url: str = ""

    redis_broker_url: str = ""
    celery_concurrency: int = 1

    kafka_bootstrap_servers: str

    @field_validator("log_level", mode="before")
    @classmethod
    def uppercase(cls, value: str) -> str:
        return value.upper() if isinstance(value, str) else value


settings = Settings()
