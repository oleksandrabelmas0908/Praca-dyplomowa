from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    service_name: str
    log_level: LogLevel = "INFO"
    env: Literal["local", "server"] = "local"

    @field_validator("log_level", mode="before")
    @classmethod
    def uppercase(cls, value: str) -> str:
        return value.upper() if isinstance(value, str) else value


settings = Settings()
