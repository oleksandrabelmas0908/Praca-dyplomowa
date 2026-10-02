import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID

import structlog
from fastapi import FastAPI, Request
from sqlalchemy import URL, DateTime, Engine, Text, create_engine, func, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from shared.settings import settings

READY_TIMEOUT_SECONDS = 1.0

logger = structlog.get_logger()


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    consumer_group: Mapped[str] = mapped_column(Text, primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


def database_url() -> URL:
    return URL.create(
        "postgresql+asyncpg",
        username=settings.postgres_user,
        password=settings.postgres_password,
        host=settings.postgres_host,
        database=settings.postgres_db,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine = create_async_engine(
        database_url(),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    app.state.engine = engine
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sync_engine: Engine | None = None
    if settings.db_driver == "sync":
        sync_engine = create_engine(
            database_url().set(drivername="postgresql+psycopg2"),
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
        )
        app.state.sync_session_factory = sessionmaker(sync_engine, expire_on_commit=False)
    yield
    await engine.dispose()
    if sync_engine is not None:
        sync_engine.dispose()


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


def get_sync_session(request: Request) -> Iterator[Session]:
    with request.app.state.sync_session_factory() as session:
        yield session


async def select_one(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def database_ready(engine: AsyncEngine) -> bool:
    try:
        async with asyncio.timeout(READY_TIMEOUT_SECONDS):
            await asyncio.shield(select_one(engine))
    except Exception as error:  # noqa: BLE001 - any failure to reach the database means not ready
        logger.warning("database not ready", error=repr(error))
        return False
    return True
