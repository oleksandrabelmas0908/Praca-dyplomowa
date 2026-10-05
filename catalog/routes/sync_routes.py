from decimal import Decimal
from typing import Annotated

from anyio import from_thread
from fastapi import APIRouter, Depends, HTTPException, Request
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from catalog import cache
from catalog.db import sync_db
from catalog.schemas import ProductListResponse, ProductResponse, ProductUpdate
from shared.db import get_sync_session

# Plain def handlers run in AnyIO's threadpool, THREADPOOL_SIZE threads per worker process
router = APIRouter()


@router.get("/products/{product_id}", response_model=ProductResponse)
@cache.cached(cache.product_key)
def get_product(
    request: Request,
    product_id: int,
    session: Annotated[Session, Depends(get_sync_session)],
) -> ProductResponse:
    product = sync_db.get_product(session, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return ProductResponse.model_validate(product)


@router.get("/products", response_model=ProductListResponse)
@cache.cached(cache.list_key)
def list_products(
    request: Request,
    session: Annotated[Session, Depends(get_sync_session)],
    category: str | None = None,
    min_price: Decimal | None = None,
    max_price: Decimal | None = None,
    limit: int = 20,
    offset: int = 0,
) -> ProductListResponse:
    products, total = sync_db.list_products(session, category, min_price, max_price, limit, offset)
    return ProductListResponse(
        items=[ProductResponse.model_validate(product) for product in products], total=total
    )


# List keys are not touched, they expire by TTL, see docs/PLAN.md
@router.patch("/products/{product_id}")
def update_product(
    request: Request,
    product_id: int,
    changes: ProductUpdate,
    session: Annotated[Session, Depends(get_sync_session)],
) -> ProductResponse:
    product = sync_db.update_product(session, product_id, changes.model_dump(exclude_none=True))
    if product is None:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    client: Redis | None = request.app.state.cache
    if client is not None:
        from_thread.run(cache.delete, client, cache.product_key(product_id))
    return ProductResponse.model_validate(product)
