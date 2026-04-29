"""Remove patron AI features (mapping task + fuzzy dedup).

Patron data is PII and is never sent to an AI/LLM. This migration drops:
  * ``patron_dedup_clusters`` table (Phase 6 fuzzy dedup output)
  * ``projects.patron_ai_task_id`` column (tracking for the patron AI
    column-mapping task)

The corresponding code paths — ``patron_ai_map_task``,
``patron_fuzzy_dedup_task``, the ``ai.fuzzy_patron_dedup`` feature flag,
the ``/patrons/maps/ai-suggest`` and ``/patrons/fuzzy-dedup`` endpoints,
the ``PatronDedupCluster`` model, and the Stage 8 Fuzzy Dedup tab — are
all removed in the same change.

Revision ID: a7u2v3w4x5y6
Revises: z6t1u2v3w4x5
Create Date: 2026-04-29
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "a7u2v3w4x5y6"
down_revision = "z6t1u2v3w4x5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_patron_dedup_clusters_project",
        table_name="patron_dedup_clusters",
        if_exists=True,
    )
    op.drop_table("patron_dedup_clusters")
    op.drop_column("projects", "patron_ai_task_id")


def downgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("patron_ai_task_id", sa.String(200), nullable=True),
    )
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
