"""add item CSV support

Revision ID: h8b3c4d5e6f7
Revises: g7a2b3c4d5e6
Create Date: 2026-03-23
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "h8b3c4d5e6f7"
down_revision: Union[str, None] = "g7a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # marc_files: add file_category and column_headers
    op.add_column(
        "marc_files",
        sa.Column("file_category", sa.String(20), server_default="marc", nullable=False),
    )
    op.add_column(
        "marc_files",
        sa.Column("column_headers", JSONB, nullable=True),
    )

    # projects: add items CSV match config
    op.add_column(
        "projects",
        sa.Column("items_csv_match_tag", sa.String(10), nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("items_csv_key_column", sa.String(200), nullable=True),
    )

    # item_column_maps table
    op.create_table(
        "item_column_maps",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("source_column", sa.String(200), nullable=False),
        sa.Column("target_subfield", sa.String(5), nullable=True),
        sa.Column("transform_type", sa.String(20), nullable=True),
        sa.Column("transform_config", JSONB, nullable=True),
        sa.Column("ignored", sa.Boolean, default=False, nullable=False, server_default="false"),
        sa.Column("approved", sa.Boolean, default=False, nullable=False, server_default="false"),
        sa.Column("ai_suggested", sa.Boolean, default=False, nullable=False, server_default="false"),
        sa.Column("ai_confidence", sa.Float, nullable=True),
        sa.Column("ai_reasoning", sa.Text, nullable=True),
        sa.Column("source_label", sa.String(50), server_default="manual", nullable=False),
        sa.Column("sort_order", sa.Integer, default=0, server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("item_column_maps")
    op.drop_column("projects", "items_csv_key_column")
    op.drop_column("projects", "items_csv_match_tag")
    op.drop_column("marc_files", "column_headers")
    op.drop_column("marc_files", "file_category")
