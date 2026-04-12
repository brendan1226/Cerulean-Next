"""Add users table and project ownership (owner_id, visibility).

Revision ID: n4h9i0j1k2l3
Revises: m3g8h9i0j1k2
Create Date: 2026-04-12
"""
from alembic import op
import sqlalchemy as sa

revision = "n4h9i0j1k2l3"
down_revision = "m3g8h9i0j1k2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Users table ───────────────────────────────────────────────────
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("email", sa.String(320), unique=True, nullable=False, index=True),
        sa.Column("name", sa.String(300), nullable=False),
        sa.Column("picture", sa.String(500), nullable=True),
        sa.Column("google_sub", sa.String(255), unique=True, nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("last_login", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )

    # ── Project ownership columns ─────────────────────────────────────
    op.add_column(
        "projects",
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("visibility", sa.String(20), server_default="private", nullable=False),
    )

    # Existing projects should be visible to everyone (shared)
    op.execute("UPDATE projects SET visibility = 'shared' WHERE owner_id IS NULL")


def downgrade() -> None:
    op.drop_column("projects", "visibility")
    op.drop_column("projects", "owner_id")
    op.drop_table("users")
