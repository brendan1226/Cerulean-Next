"""add transform_manifests table

Revision ID: 419094484aee
Revises:
Create Date: 2026-03-08
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "419094484aee"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "transform_manifests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("task_type", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), server_default="running", nullable=False),
        sa.Column("celery_task_id", sa.String(200), nullable=True),
        sa.Column("output_path", sa.Text(), nullable=True),
        sa.Column("files_processed", sa.Integer(), nullable=True),
        sa.Column("total_records", sa.Integer(), nullable=True),
        sa.Column("records_skipped", sa.Integer(), nullable=True),
        sa.Column("items_joined", sa.Integer(), nullable=True),
        sa.Column("duplicate_001s", postgresql.JSONB(), nullable=True),
        sa.Column("file_ids", postgresql.JSONB(), nullable=True),
        sa.Column("items_csv_path", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("transform_manifests")
