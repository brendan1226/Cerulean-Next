"""add archived and subfield_frequency columns

Revision ID: c3f8a1d2e5b7
Revises: b7e3a2f1c4d8
Create Date: 2026-03-08
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3f8a1d2e5b7"
down_revision: Union[str, None] = "b7e3a2f1c4d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("archived", sa.Boolean(), server_default="false", nullable=False),
    )
    op.add_column(
        "marc_files",
        sa.Column("subfield_frequency", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("marc_files", "subfield_frequency")
    op.drop_column("projects", "archived")
