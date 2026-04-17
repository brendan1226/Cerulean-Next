"""Add patron_dedup_clusters table for Phase 6 AI Fuzzy Patron Dedup.

Patron dedup clusters are separate from the MARC bib DedupCluster table
because patron data is CSV-based (row indices, column values) rather
than MARC-based (record index, field count, item count). The table
stores AI-scored candidate pairs with confidence + reasoning for
engineer review.

Revision ID: y5s0t1u2v3w4
Revises: x4r9s0t1u2v3
Create Date: 2026-04-16
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "y5s0t1u2v3w4"
down_revision = "x4r9s0t1u2v3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "patron_dedup_clusters",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("records", JSONB, nullable=False),
        sa.Column("primary_index", sa.Integer, default=0),
        sa.Column("match_key", sa.String(200)),
        sa.Column("confidence", sa.Float),
        sa.Column("reasoning", sa.Text),
        sa.Column("resolved", sa.Boolean, default=False, server_default=sa.false()),
        sa.Column("dismissed", sa.Boolean, default=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_patron_dedup_clusters_project",
        "patron_dedup_clusters",
        ["project_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_patron_dedup_clusters_project")
    op.drop_table("patron_dedup_clusters")
