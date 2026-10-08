"""products stock_reserved

Revision ID: 913556dd5acb
Revises: 3c8e1f5a9d27
Create Date: 2026-10-08 12:45:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "913556dd5acb"
down_revision: str | Sequence[str] | None = "3c8e1f5a9d27"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "products",
        sa.Column("stock_reserved", sa.Integer(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("products", "stock_reserved")
