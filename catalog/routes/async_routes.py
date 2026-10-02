from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from catalog.db import async_db
from catalog.schemas import ProductListResponse, ProductResponse
from shared.db import get_session

router = APIRouter()


@router.get("/products/{product_id}")
async def get_product(
    product_id: int,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ProductResponse:
    product = await async_db.get_product(session, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return ProductResponse.model_validate(product)


@router.get("/products")
async def list_products(
    session: Annotated[AsyncSession, Depends(get_session)],
    category: str | None = None,
    min_price: Decimal | None = None,
    max_price: Decimal | None = None,
    limit: int = 20,
    offset: int = 0,
) -> ProductListResponse:
    products, total = await async_db.list_products(
        session, category, min_price, max_price, limit, offset
    )
    return ProductListResponse(
        items=[ProductResponse.model_validate(product) for product in products], total=total
    )
