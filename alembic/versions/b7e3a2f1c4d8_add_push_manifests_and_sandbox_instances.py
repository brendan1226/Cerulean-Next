"""add push_manifests and sandbox_instances tables

Revision ID: b7e3a2f1c4d8
Revises: 419094484aee
Create Date: 2026-03-08
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7e3a2f1c4d8"
down_revision: Union[str, None] = "419094484aee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "push_manifests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("task_type", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), server_default="running", nullable=False),
        sa.Column("celery_task_id", sa.String(200), nullable=True),
        sa.Column("dry_run", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("records_total", sa.Integer(), nullable=True),
        sa.Column("records_success", sa.Integer(), nullable=True),
        sa.Column("records_failed", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("result_data", postgresql.JSONB(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "sandbox_instances",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("container_id", sa.String(100), nullable=True),
        sa.Column("container_name", sa.String(200), nullable=True),
        sa.Column("status", sa.String(20), server_default="provisioning", nullable=False),
        sa.Column("koha_url", sa.String(500), nullable=True),
        sa.Column("exposed_port", sa.Integer(), nullable=True),
        sa.Column("celery_task_id", sa.String(200), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("sandbox_instances")
    op.drop_table("push_manifests")
