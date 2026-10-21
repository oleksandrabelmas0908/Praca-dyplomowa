"""products drop image_paths

Revision ID: a4c7e2b9f1d3
Revises: 2e7b9c4d5f18
Create Date: 2026-10-21 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a4c7e2b9f1d3"
down_revision: str | Sequence[str] | None = "2e7b9c4d5f18"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("products", "image_paths")


def downgrade() -> None:
    op.add_column(
        "products",
        sa.Column(
            "image_paths",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
    )
