"""reservations

Revision ID: b6d3f8a1c2e4
Revises: a4c7e2b9f1d3
Create Date: 2026-10-26 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b6d3f8a1c2e4"
down_revision: str | Sequence[str] | None = "a4c7e2b9f1d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

reservation_status = sa.Enum("reserved", "released", name="reservation_status")


def upgrade() -> None:
    op.create_table(
        "reservations",
        sa.Column("order_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("lines", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", reservation_status, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("order_id"),
    )


def downgrade() -> None:
    op.drop_table("reservations")
    reservation_status.drop(op.get_bind())
