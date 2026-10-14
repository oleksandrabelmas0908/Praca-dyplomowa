"""invoices completed and failed

Revision ID: 2e7b9c4d5f18
Revises: 8d4f2a6c13e5
Create Date: 2026-10-14 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2e7b9c4d5f18"
down_revision: str | Sequence[str] | None = "8d4f2a6c13e5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TYPE invoice_status RENAME VALUE 'generated' TO 'completed'")
    op.execute("ALTER TYPE invoice_status ADD VALUE 'failed'")
    op.add_column("invoices", sa.Column("error", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("invoices", "error")
    # Postgres cannot drop a value from an enum, so the type is recreated without it
    op.execute("ALTER TYPE invoice_status RENAME TO invoice_status_old")
    op.execute("CREATE TYPE invoice_status AS ENUM ('pending', 'generated')")
    op.execute(
        "ALTER TABLE invoices ALTER COLUMN status TYPE invoice_status USING (CASE status::text "
        "WHEN 'completed' THEN 'generated' WHEN 'failed' THEN 'pending' ELSE status::text END"
        ")::invoice_status"
    )
    op.execute("DROP TYPE invoice_status_old")
