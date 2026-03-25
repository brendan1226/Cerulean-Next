"""Widen source_value columns from varchar(500) to text.

Revision ID: i9c4d5e6f7g8
Revises: h8b3c4d5e6f7
Create Date: 2026-03-24
"""
from alembic import op
import sqlalchemy as sa

revision = "i9c4d5e6f7g8"
down_revision = "h8b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("reconciliation_scan_results", "source_value",
                    existing_type=sa.String(500), type_=sa.Text(), nullable=False)
    op.alter_column("patron_scan_results", "source_value",
                    existing_type=sa.String(500), type_=sa.Text(), nullable=False)


def downgrade() -> None:
    op.alter_column("reconciliation_scan_results", "source_value",
                    existing_type=sa.Text(), type_=sa.String(500), nullable=False)
    op.alter_column("patron_scan_results", "source_value",
                    existing_type=sa.Text(), type_=sa.String(500), nullable=False)
