"""processed_events

Revision ID: 7e1d0bcb49dd
Revises:
Create Date: 2026-09-24 08:23:20.789890

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7e1d0bcb49dd"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("consumer_group", sa.Text(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("event_id", "consumer_group"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    op.drop_table("processed_events")
