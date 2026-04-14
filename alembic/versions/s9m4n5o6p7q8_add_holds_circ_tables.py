"""Add id_mappings, holds_files tables, and circ_count column.

Revision ID: s9m4n5o6p7q8
Revises: r8l3m4n5o6p7
Create Date: 2026-04-14
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "s9m4n5o6p7q8"
down_revision = "r8l3m4n5o6p7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ID Mappings table — legacy ILS ID → Koha ID
    op.create_table(
        "id_mappings",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False, index=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("legacy_id", sa.String(200), nullable=False),
        sa.Column("koha_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("project_id", "entity_type", "legacy_id", name="uq_id_mapping"),
    )
    op.create_index("ix_id_mapping_lookup", "id_mappings", ["project_id", "entity_type", "legacy_id"])

    # Holds files table — uploaded holds/circ CSV files
    op.create_table(
        "holds_files",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("filename", sa.String(300), nullable=False),
        sa.Column("file_category", sa.String(20), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("column_headers", JSONB(), nullable=True),
        sa.Column("status", sa.String(20), server_default="uploaded", nullable=False),
        sa.Column("validation_result", JSONB(), nullable=True),
        sa.Column("storage_path", sa.Text(), nullable=False),
        sa.Column("uploaded_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )

    # Add circ_count to projects
    op.add_column("projects", sa.Column("circ_count", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("projects", "circ_count")
    op.drop_table("holds_files")
    op.drop_index("ix_id_mapping_lookup", "id_mappings")
    op.drop_table("id_mappings")
