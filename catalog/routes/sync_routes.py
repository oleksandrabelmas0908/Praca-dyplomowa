from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from catalog.db import sync_db
from catalog.schemas import ProductListResponse, ProductResponse
from shared.db import get_sync_session

# Plain def handlers run in AnyIO's threadpool, whose default limit is 40 threads per worker
# process. Deliberately left at the default, not tuned for E1
router = APIRouter()


@router.get("/products/{product_id}")
def get_product(
    product_id: int,
    session: Annotated[Session, Depends(get_sync_session)],
) -> ProductResponse:
    product = sync_db.get_product(session, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return ProductResponse.model_validate(product)


@router.get("/products")
def list_products(
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
