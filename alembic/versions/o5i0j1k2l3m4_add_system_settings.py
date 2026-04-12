"""Add system_settings table for GUI-configurable platform settings.

Revision ID: o5i0j1k2l3m4
Revises: n4h9i0j1k2l3
Create Date: 2026-04-12
"""
from alembic import op
import sqlalchemy as sa

revision = "o5i0j1k2l3m4"
down_revision = "n4h9i0j1k2l3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "system_settings",
        sa.Column("key", sa.String(100), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False, server_default=""),
        sa.Column("is_secret", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("system_settings")
