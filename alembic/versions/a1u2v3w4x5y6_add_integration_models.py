"""Add Integration API models: api_keys, control_value_sets, integration_push_logs.

Also adds a ``source`` column to marc_files and patron_files so the UI
can distinguish between files uploaded directly vs pushed from the Koha
Migration Workbench or other integrations.

Revision ID: a1u2v3w4x5y6
Revises: z6t1u2v3w4x5
Create Date: 2026-04-17
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "a1u2v3w4x5y6"
down_revision = "z6t1u2v3w4x5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Source tracking on file models
    op.add_column(
        "marc_files",
        sa.Column("source", sa.String(50), nullable=False, server_default="upload"),
    )
    op.add_column(
        "patron_files",
        sa.Column("source", sa.String(50), nullable=False, server_default="upload"),
    )

    # API Keys
    op.create_table(
        "api_keys",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("key_hash", sa.String(128), nullable=False, unique=True),
        sa.Column("key_prefix", sa.String(20), nullable=False),
        sa.Column("label", sa.String(200), nullable=False),
        sa.Column("scope", sa.String(20), nullable=False, server_default="write"),
        sa.Column("last_used_at", sa.DateTime, nullable=True),
        sa.Column("revoked", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index("ix_api_keys_key_hash", "api_keys", ["key_hash"])
    op.create_index("ix_api_keys_user", "api_keys", ["user_id"])

    # Control Value Sets
    op.create_table(
        "control_value_sets",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("cv_type", sa.String(50), nullable=False),
        sa.Column("code", sa.String(200), nullable=False),
        sa.Column("description", sa.String(500)),
        sa.Column("source", sa.String(50), nullable=False, server_default="manual"),
        sa.Column("raw_sql_path", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index("ix_control_value_sets_project", "control_value_sets", ["project_id"])
    op.create_index("ix_control_value_sets_project_type", "control_value_sets", ["project_id", "cv_type"])

    # Integration Push Logs
    op.create_table(
        "integration_push_logs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("api_key_id", sa.String(36), sa.ForeignKey("api_keys.id"), nullable=False),
        sa.Column("push_type", sa.String(20), nullable=False),
        sa.Column("files_received", JSONB),
        sa.Column("status", sa.String(20), nullable=False, server_default="received"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index("ix_integration_push_logs_project", "integration_push_logs", ["project_id"])


def downgrade() -> None:
    op.drop_table("integration_push_logs")
    op.drop_table("control_value_sets")
    op.drop_table("api_keys")
    op.drop_column("patron_files", "source")
    op.drop_column("marc_files", "source")
