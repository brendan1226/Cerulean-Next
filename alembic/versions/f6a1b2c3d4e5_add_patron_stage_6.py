"""add patron stage 6

Revision ID: f6a1b2c3d4e5
Revises: e5f0c3d4a7b2
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f6a1b2c3d4e5"
down_revision: Union[str, None] = "e5f0c3d4a7b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create patron_files table
    op.create_table(
        "patron_files",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("filename", sa.String(300), nullable=False),
        sa.Column("file_format", sa.String(20)),  # csv | tsv | patron_marc | xlsx | xls | xml | fixed
        sa.Column("detected_format", sa.String(20)),
        sa.Column("parse_settings", JSONB, nullable=True),
        sa.Column("row_count", sa.Integer, nullable=True),
        sa.Column("column_headers", JSONB, nullable=True),
        sa.Column("status", sa.String(20), server_default="uploaded"),
        sa.Column("uploaded_at", sa.DateTime, server_default=sa.func.now()),
    )

    # 2. Create patron_column_maps table
    op.create_table(
        "patron_column_maps",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("source_column", sa.String(200), nullable=False),
        sa.Column("target_header", sa.String(200), nullable=True),
        sa.Column("ignored", sa.Boolean, server_default="false"),
        sa.Column("transform_type", sa.String(20), nullable=True),
        sa.Column("transform_config", JSONB, nullable=True),
        sa.Column("is_controlled_list", sa.Boolean, server_default="false"),
        sa.Column("approved", sa.Boolean, server_default="false"),
        sa.Column("ai_suggested", sa.Boolean, server_default="false"),
        sa.Column("ai_confidence", sa.Float, nullable=True),
        sa.Column("ai_reasoning", sa.Text, nullable=True),
        sa.Column("source_label", sa.String(50), server_default="manual"),
        sa.Column("sort_order", sa.Integer, default=0),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now()),
    )

    # 3. Create patron_value_rules table
    op.create_table(
        "patron_value_rules",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("koha_header", sa.String(50), nullable=False),
        sa.Column("operation", sa.String(20), nullable=False),  # rename | merge | split | delete
        sa.Column("source_values", JSONB, nullable=False),
        sa.Column("target_value", sa.String(200), nullable=True),
        sa.Column("split_conditions", JSONB, nullable=True),
        sa.Column("delete_mode", sa.String(20), nullable=True),  # exclude_row | blank_field
        sa.Column("sort_order", sa.Integer, default=0),
        sa.Column("active", sa.Boolean, server_default="true"),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now()),
    )

    # 4. Create patron_scan_results table
    op.create_table(
        "patron_scan_results",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("koha_header", sa.String(50), nullable=False),
        sa.Column("source_value", sa.String(500), nullable=False),
        sa.Column("record_count", sa.Integer, nullable=False),
        sa.Column("scanned_at", sa.DateTime, server_default=sa.func.now()),
    )

    # 5. Add new columns to projects
    op.add_column("projects", sa.Column("stage_8_complete", sa.Boolean, server_default="false", nullable=False))
    op.add_column("projects", sa.Column("patron_file_id", sa.String(36), nullable=True))
    op.add_column("projects", sa.Column("patron_scan_task_id", sa.String(200), nullable=True))
    op.add_column("projects", sa.Column("patron_apply_task_id", sa.String(200), nullable=True))
    op.add_column("projects", sa.Column("patron_ai_task_id", sa.String(200), nullable=True))

    # 6. Renumber existing audit events: stage 7 (sandbox) → 8, then stage 6 (push) → 7
    #    Order matters: do 7→8 first, then 6→7, to avoid collision
    op.execute("UPDATE audit_events SET stage = 8 WHERE stage = 7")
    op.execute("UPDATE audit_events SET stage = 7 WHERE stage = 6")


def downgrade() -> None:
    # Reverse audit event renumbering: 7→6, then 8→7
    op.execute("UPDATE audit_events SET stage = 6 WHERE stage = 7")
    op.execute("UPDATE audit_events SET stage = 7 WHERE stage = 8")

    op.drop_column("projects", "patron_ai_task_id")
    op.drop_column("projects", "patron_apply_task_id")
    op.drop_column("projects", "patron_scan_task_id")
    op.drop_column("projects", "patron_file_id")
    op.drop_column("projects", "stage_8_complete")

    op.drop_table("patron_scan_results")
    op.drop_table("patron_value_rules")
    op.drop_table("patron_column_maps")
    op.drop_table("patron_files")
