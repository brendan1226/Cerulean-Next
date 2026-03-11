"""add koha_auth_type to projects

Revision ID: g7a2b3c4d5e6
Revises: f6a1b2c3d4e5
Create Date: 2026-03-11
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "g7a2b3c4d5e6"
down_revision: Union[str, None] = "f6a1b2c3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("koha_auth_type", sa.String(20), server_default="basic", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("projects", "koha_auth_type")
