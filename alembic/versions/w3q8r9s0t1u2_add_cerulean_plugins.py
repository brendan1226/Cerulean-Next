"""Add cerulean_plugins table.

Distinct from the existing ``plugins`` table (which holds Koha ``.kpz``
packages pushed TO a Koha instance). This table tracks plugins that
extend Cerulean itself — custom transforms, quality checks, and
eventually UI tabs — delivered as ``.cpz`` archives.

Revision ID: w3q8r9s0t1u2
Revises: v2p7q8r9s0t1
Create Date: 2026-04-16
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "w3q8r9s0t1u2"
down_revision = "v2p7q8r9s0t1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cerulean_plugins",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("slug", sa.String(100), nullable=False, unique=True, index=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("version", sa.String(50), nullable=False),
        sa.Column("author", sa.String(200), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("runtime", sa.String(20), nullable=False),
        sa.Column("manifest", JSONB, nullable=False),
        sa.Column("install_path", sa.Text(), nullable=False),
        sa.Column("archive_filename", sa.String(300), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="enabled"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("installed_by", sa.String(36), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("installed_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("cerulean_plugins")
