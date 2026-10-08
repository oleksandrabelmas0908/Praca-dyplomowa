from enum import StrEnum

from pydantic import BaseModel


class RejectionReason(StrEnum):
    insufficient_stock = "insufficient_stock"
    product_not_found = "product_not_found"


class Rejection(BaseModel):
    reason: RejectionReason
    product_id: int
    requested: int
    available: int
