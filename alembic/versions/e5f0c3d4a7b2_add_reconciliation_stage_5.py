"""add reconciliation stage 5

Revision ID: e5f0c3d4a7b2
Revises: d4e9b2c3f6a1
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5f0c3d4a7b2"
down_revision: Union[str, None] = "d4e9b2c3f6a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create reconciliation_rules table
    op.create_table(
        "reconciliation_rules",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("vocab_category", sa.String(50), nullable=False),
        sa.Column("marc_tag", sa.String(10), nullable=False),
        sa.Column("marc_subfield", sa.String(5), nullable=False),
        sa.Column("operation", sa.String(20), nullable=False),  # rename | merge | split | delete
        sa.Column("source_values", JSONB, nullable=False),       # list of source values
        sa.Column("target_value", sa.String(200), nullable=True),
        sa.Column("split_conditions", JSONB, nullable=True),     # for split operations
        sa.Column("delete_mode", sa.String(20), nullable=True),  # subfield | field
        sa.Column("sort_order", sa.Integer, default=0),
        sa.Column("active", sa.Boolean, default=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now()),
    )

    # 2. Create reconciliation_scan_results table
    op.create_table(
        "reconciliation_scan_results",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("vocab_category", sa.String(50), nullable=False),
        sa.Column("source_value", sa.String(500), nullable=False),
        sa.Column("record_count", sa.Integer, nullable=False),
        sa.Column("scanned_at", sa.DateTime, server_default=sa.func.now()),
    )

    # 3. Add new columns to projects
    op.add_column("projects", sa.Column("stage_7_complete", sa.Boolean, server_default="false", nullable=False))
    op.add_column("projects", sa.Column("reconcile_source_file", sa.String(500), nullable=True))
    op.add_column("projects", sa.Column("reconcile_scan_task_id", sa.String(200), nullable=True))
    op.add_column("projects", sa.Column("reconcile_apply_task_id", sa.String(200), nullable=True))

    # 4. Renumber existing audit events: stage 5 (push) → 6, stage 6 (sandbox) → 7
    #    Order matters: do 6→7 first, then 5→6, to avoid collision
    op.execute("UPDATE audit_events SET stage = 7 WHERE stage = 6")
    op.execute("UPDATE audit_events SET stage = 6 WHERE stage = 5")


def downgrade() -> None:
    # Reverse audit event renumbering: 6→5, then 7→6
    op.execute("UPDATE audit_events SET stage = 5 WHERE stage = 6")
    op.execute("UPDATE audit_events SET stage = 6 WHERE stage = 7")

    op.drop_column("projects", "reconcile_apply_task_id")
    op.drop_column("projects", "reconcile_scan_task_id")
    op.drop_column("projects", "reconcile_source_file")
    op.drop_column("projects", "stage_7_complete")

    op.drop_table("reconciliation_scan_results")
    op.drop_table("reconciliation_rules")
