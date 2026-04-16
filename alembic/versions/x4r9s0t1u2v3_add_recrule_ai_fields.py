"""Add AI suggestion columns to reconciliation_rules.

Backs the Phase 5 AI Code Reconciliation flow (cerulean_ai_spec.md §6).
An AI-generated suggestion lands as an *inactive* ReconciliationRule row
with ai_suggested=True + confidence + reasoning. The migrator reviews
the suggestion on the Rules tab; approving flips active=True and the
existing apply task handles the transform — no new execution path.

Revision ID: x4r9s0t1u2v3
Revises: w3q8r9s0t1u2
Create Date: 2026-04-16
"""
from alembic import op
import sqlalchemy as sa

revision = "x4r9s0t1u2v3"
down_revision = "w3q8r9s0t1u2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reconciliation_rules",
        sa.Column("ai_suggested", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "reconciliation_rules",
        sa.Column("ai_confidence", sa.Float(), nullable=True),
    )
    op.add_column(
        "reconciliation_rules",
        sa.Column("ai_reasoning", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("reconciliation_rules", "ai_reasoning")
    op.drop_column("reconciliation_rules", "ai_confidence")
    op.drop_column("reconciliation_rules", "ai_suggested")
