"""Add plugins table for Koha plugin manager.

Revision ID: p6j1k2l3m4n5
Revises: o5i0j1k2l3m4
Create Date: 2026-04-12
"""
from alembic import op
import sqlalchemy as sa

revision = "p6j1k2l3m4n5"
down_revision = "o5i0j1k2l3m4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "plugins",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("filename", sa.String(300), nullable=False),
        sa.Column("display_name", sa.String(300), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("version", sa.String(50), nullable=True),
        sa.Column("storage_path", sa.Text(), nullable=False),
        sa.Column("file_size_bytes", sa.Integer(), nullable=True),
        sa.Column("uploaded_by", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("plugins")
