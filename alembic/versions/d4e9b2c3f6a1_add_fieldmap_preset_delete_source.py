"""add preset_key and delete_source to field_maps

Revision ID: d4e9b2c3f6a1
Revises: c3f8a1d2e5b7
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4e9b2c3f6a1"
down_revision: Union[str, None] = "c3f8a1d2e5b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "field_maps",
        sa.Column("preset_key", sa.String(50), nullable=True),
    )
    op.add_column(
        "field_maps",
        sa.Column("delete_source", sa.Boolean(), server_default="false", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("field_maps", "delete_source")
    op.drop_column("field_maps", "preset_key")
