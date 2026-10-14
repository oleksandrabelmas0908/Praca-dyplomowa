"""invoices status

Revision ID: 8d4f2a6c13e5
Revises: 5b2e9a7c41d8
Create Date: 2026-10-14 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "8d4f2a6c13e5"
down_revision: str | Sequence[str] | None = "5b2e9a7c41d8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

invoice_status = sa.Enum("pending", "generated", name="invoice_status")


def upgrade() -> None:
    invoice_status.create(op.get_bind())
    # Rows from before this revision already have their PDF
    op.add_column(
        "invoices",
        sa.Column("status", invoice_status, server_default="generated", nullable=False),
    )
    op.alter_column("invoices", "status", server_default=None)


def downgrade() -> None:
    op.drop_column("invoices", "status")
    invoice_status.drop(op.get_bind())
