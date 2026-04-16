"""Add user_preferences table.

Generic per-user key/value store that backs AI feature flags today and
other granular visibility toggles tomorrow. Keys, defaults, and UI
metadata live in cerulean/core/features.py — this migration only creates
the storage.

Revision ID: t0n5o6p7q8r9
Revises: s9m4n5o6p7q8
Create Date: 2026-04-15
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "t0n5o6p7q8r9"
down_revision = "s9m4n5o6p7q8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_preferences",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "user_id",
            sa.String(36),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("key", sa.String(100), nullable=False, index=True),
        sa.Column("value", JSONB, nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("user_id", "key", name="uq_user_preference"),
    )


def downgrade() -> None:
    op.drop_table("user_preferences")
