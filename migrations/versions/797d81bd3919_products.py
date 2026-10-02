"""products

Revision ID: 797d81bd3919
Revises: 7e1d0bcb49dd
Create Date: 2026-10-01 10:48:41.578034

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "797d81bd3919"
down_revision: str | Sequence[str] | None = "7e1d0bcb49dd"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "products",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("stock_quantity", sa.Integer(), nullable=False),
        sa.Column(
            "image_paths",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_products_category_id", "products", ["category", "id"])


def downgrade() -> None:
    op.drop_table("products")
