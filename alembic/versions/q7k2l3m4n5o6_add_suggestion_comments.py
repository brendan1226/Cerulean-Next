"""Add suggestion_comments table.

Revision ID: q7k2l3m4n5o6
Revises: p6j1k2l3m4n5
Create Date: 2026-04-13
"""
from alembic import op
import sqlalchemy as sa

revision = "q7k2l3m4n5o6"
down_revision = "p6j1k2l3m4n5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "suggestion_comments",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("suggestion_id", sa.String(36), sa.ForeignKey("suggestions.id"), nullable=False),
        sa.Column("author_email", sa.String(320), nullable=False),
        sa.Column("author_name", sa.String(300), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("suggestion_comments")
