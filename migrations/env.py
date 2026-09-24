import asyncio

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

# Autogenerate only sees tables whose models have been imported. shared.db brings processed_events;
# add each service's models module here when that service gets its first table.
from shared.db import Base, database_url
from shared.logging import setup_logging
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)


def run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def main() -> None:
    engine = create_async_engine(database_url(), poolclass=NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(run_migrations)
    await engine.dispose()


asyncio.run(main())
