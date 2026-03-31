"""Add item_structure column to projects for Stage 2 config.

Revision ID: m3g8h9i0j1k2
Revises: l2f7g8h9i0j1
Create Date: 2026-03-31
"""
from alembic import op
import sqlalchemy as sa

revision = "m3g8h9i0j1k2"
down_revision = "l2f7g8h9i0j1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("projects", sa.Column("item_structure", sa.String(20)))


def downgrade() -> None:
    op.drop_column("projects", "item_structure")
