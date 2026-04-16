"""Add ai_prompt column to field_maps.

Stores the user's plain-English description of a transform when the AI
Transform Rule Generation flow (cerulean_ai_spec.md §5) produced the
generated expression. Keeping the prompt alongside the code gives
traceability in the audit log and lets the edit modal show the author's
original intent, not just the opaque expression.

Revision ID: v2p7q8r9s0t1
Revises: u1o6p7q8r9s0
Create Date: 2026-04-15
"""
from alembic import op
import sqlalchemy as sa

revision = "v2p7q8r9s0t1"
down_revision = "u1o6p7q8r9s0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("field_maps", sa.Column("ai_prompt", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("field_maps", "ai_prompt")
