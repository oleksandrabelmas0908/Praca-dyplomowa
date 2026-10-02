import asyncio
import random
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import func, insert, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from catalog.db import Product
from shared.db import database_url
from shared.logging import setup_logging
from shared.settings import settings

# Sized so the table is well over Postgres' 128 MB shared_buffers, see docs/PLAN.md
PRODUCT_COUNT = 100_000
# Batches keep memory flat inside the catalog container
BATCH_SIZE = 5_000
# Experiment runs are compared across days, so every seed must produce the same catalogue.
# For the same reason timestamps are a fixed date, not now()
RANDOM_SEED = 5
CREATED_AT = datetime(2026, 1, 1, tzinfo=UTC)

CATEGORIES = {
    "Electronics": ("Headphones", "Speaker", "Power Bank", "Webcam"),
    "Kitchen": ("Kettle", "Blender", "Frying Pan", "Coffee Grinder"),
    "Home": ("Table Lamp", "Wall Clock", "Throw Blanket", "Candle"),
    "Furniture": ("Office Chair", "Bookshelf", "Desk", "Coffee Table"),
    "Garden": ("Garden Hose", "Planter", "Pruning Shears", "Bird Feeder"),
    "Tools": ("Cordless Drill", "Socket Set", "Tape Measure", "Workbench"),
    "Sports": ("Yoga Mat", "Dumbbell Set", "Jump Rope", "Kettlebell"),
    "Clothing": ("Rain Jacket", "Hoodie", "Sweater", "Denim Shirt"),
    "Toys": ("Puzzle", "Board Game", "Plush Bear", "Train Set"),
    "Office": ("Notebook", "Fountain Pen", "Desk Organiser", "Whiteboard"),
}
ADJECTIVES = ("Compact", "Classic", "Pro", "Lightweight", "Premium", "Everyday", "Deluxe", "Eco")
SENTENCES = (
    "Designed for everyday use, it combines solid build quality with a clean, simple look.",
    "Every unit is inspected by hand before it leaves our warehouse.",
    "It ships in plastic-free packaging made from recycled cardboard.",
    "A two-year warranty covers manufacturing defects, and spare parts are sold separately.",
    "The surface wipes clean with a damp cloth, so maintenance stays simple.",
    "Independent reviewers have praised its build quality and attention to detail.",
    "If it does not suit you, return it within 30 days for a full refund.",
    "Setup takes a couple of minutes and needs no extra tools or prior experience.",
    "Compared with the previous model, this version is lighter and takes up less space.",
    "Our support team answers questions about it seven days a week.",
)

setup_logging(settings.service_name, settings.log_level)
logger = structlog.get_logger()


def product(rng: random.Random) -> dict[str, Any]:
    category = rng.choice(list(CATEGORIES))
    name = f"{rng.choice(ADJECTIVES)} {rng.choice(CATEGORIES[category])} {rng.randint(100, 999)}"
    # 1,400-1,800 characters: a realistic row size that still stays under Postgres' ~2 kB TOAST
    # threshold, so rows are stored inline and uncompressed
    description = " ".join(rng.choices(SENTENCES, k=30))[: rng.randint(1_400, 1_800)]
    return {
        "name": name,
        "description": description,
        "price": Decimal(rng.randint(199, 99_999)) / 100,
        "category": category,
        "stock_quantity": rng.randint(0, 50),
        "created_at": CREATED_AT,
        "updated_at": CREATED_AT,
    }


async def main() -> None:
    started = time.perf_counter()
    rng = random.Random(RANDOM_SEED)
    engine = create_async_engine(database_url(), poolclass=NullPool)
    # One transaction, so a failed seed leaves the previous catalogue in place. TRUNCATE rather
    # than refusing, so re-running restores the exact catalogue after an experiment changed it
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE products RESTART IDENTITY"))
        for _ in range(PRODUCT_COUNT // BATCH_SIZE):
            await connection.execute(insert(Product), [product(rng) for _ in range(BATCH_SIZE)])
        rows = await connection.scalar(select(func.count()).select_from(Product))
    await engine.dispose()
    logger.info("products seeded", rows=rows, seconds=round(time.perf_counter() - started, 1))


if __name__ == "__main__":
    asyncio.run(main())
