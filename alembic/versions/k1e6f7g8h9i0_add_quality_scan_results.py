"""Add quality_scan_results table for Stage 3 MARC Data Quality.

Revision ID: k1e6f7g8h9i0
Revises: j0d5e6f7g8h9
Create Date: 2026-03-31
"""
from alembic import op
import sqlalchemy as sa

revision = "k1e6f7g8h9i0"
down_revision = "j0d5e6f7g8h9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "quality_scan_results",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False, index=True),
        sa.Column("scan_type", sa.String(20), nullable=False),
        sa.Column("category", sa.String(40), nullable=False, index=True),
        sa.Column("severity", sa.String(10), nullable=False),
        sa.Column("record_index", sa.Integer, nullable=False),
        sa.Column("file_id", sa.String(36), sa.ForeignKey("marc_files.id")),
        sa.Column("tag", sa.String(5)),
        sa.Column("subfield", sa.String(5)),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("original_value", sa.Text),
        sa.Column("suggested_fix", sa.Text),
        sa.Column("status", sa.String(20), server_default="unresolved"),
        sa.Column("fixed_value", sa.Text),
        sa.Column("fixed_by", sa.String(200)),
        sa.Column("fixed_at", sa.DateTime),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("quality_scan_results")
