"""Add health_report columns to marc_files.

Stores the AI-produced Data Health Report (cerulean_ai_spec.md §3)
alongside the MARC file it describes. Four columns:

  * health_report              — JSONB payload (findings array + metadata)
  * health_report_status       — "running" | "ready" | "error"
  * health_report_error        — short error string when the task fails
  * health_report_generated_at — timestamp of the last successful run

Columns are nullable so the feature stays opt-in: a user who never
enables ai.data_health_report simply sees NULL everywhere.

Revision ID: u1o6p7q8r9s0
Revises: t0n5o6p7q8r9
Create Date: 2026-04-15
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "u1o6p7q8r9s0"
down_revision = "t0n5o6p7q8r9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("marc_files", sa.Column("health_report", JSONB, nullable=True))
    op.add_column("marc_files", sa.Column("health_report_status", sa.String(20), nullable=True))
    op.add_column("marc_files", sa.Column("health_report_error", sa.Text(), nullable=True))
    op.add_column("marc_files", sa.Column("health_report_generated_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("marc_files", "health_report_generated_at")
    op.drop_column("marc_files", "health_report_error")
    op.drop_column("marc_files", "health_report_status")
    op.drop_column("marc_files", "health_report")
