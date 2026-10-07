from collections import Counter
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator

from orders.models import OrderStatus


class OrderLineCreate(BaseModel):
    product_id: int
    quantity: PositiveInt


class OrderCreate(BaseModel):
    customer_id: int
    lines: list[OrderLineCreate] = Field(min_length=1)

    @field_validator("lines")
    @classmethod
    def unique_products(cls, lines: list[OrderLineCreate]) -> list[OrderLineCreate]:
        counts = Counter(line.product_id for line in lines)
        duplicates = sorted(product_id for product_id, count in counts.items() if count > 1)
        if duplicates:
            raise ValueError(f"duplicate product_id: {', '.join(map(str, duplicates))}")
        return lines


class OrderLineResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    product_id: int
    quantity: int
    unit_price: Decimal


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    customer_id: int
    status: OrderStatus
    total_amount: Decimal
    created_at: datetime
    updated_at: datetime
    lines: list[OrderLineResponse]
