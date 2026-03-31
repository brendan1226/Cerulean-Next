"""Add migration_versions table for Stage 4 versioned runs.

Revision ID: l2f7g8h9i0j1
Revises: k1e6f7g8h9i0
Create Date: 2026-03-31
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "l2f7g8h9i0j1"
down_revision = "k1e6f7g8h9i0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "migration_versions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False, index=True),
        sa.Column("data_type", sa.String(20), nullable=False),
        sa.Column("version_number", sa.Integer, nullable=False),
        sa.Column("label", sa.String(200)),
        sa.Column("snapshot_path", sa.Text, nullable=False),
        sa.Column("mapping_snapshot", JSONB),
        sa.Column("quality_log", JSONB),
        sa.Column("record_count", sa.Integer, server_default="0"),
        sa.Column("run_duration_seconds", sa.Integer),
        sa.Column("load_success", sa.Integer),
        sa.Column("load_failed", sa.Integer),
        sa.Column("created_by", sa.String(200)),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("migration_versions")
