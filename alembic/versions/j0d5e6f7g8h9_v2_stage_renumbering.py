"""v2.0 pipeline stage renumbering: 8 stages → 11 stages.

Adds stage_9_complete, stage_10_complete, stage_11_complete columns.
Remaps existing stage completion flags and current_stage values:
  old 3 (Build) → new 6 (Transform)
  old 5 (Recon) → new 8 (Reconciliation)
  old 6 (Patrons) → new 9 (Patrons)
  old 8 (Sandbox) → new 11 (Holds)
  old 1, 2, 4, 7 stay the same.
  New 3 (Quality), 5 (Field Mapping), 10 (Patron Versioning) start as false/empty.

Revision ID: j0d5e6f7g8h9
Revises: i9c4d5e6f7g8
Create Date: 2026-03-31
"""
from alembic import op
import sqlalchemy as sa

revision = "j0d5e6f7g8h9"
down_revision = "i9c4d5e6f7g8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add new stage columns
    op.add_column("projects", sa.Column("stage_9_complete", sa.Boolean(), nullable=False, server_default="false"))
    op.add_column("projects", sa.Column("stage_10_complete", sa.Boolean(), nullable=False, server_default="false"))
    op.add_column("projects", sa.Column("stage_11_complete", sa.Boolean(), nullable=False, server_default="false"))

    conn = op.get_bind()

    # 2. Remap stage completion flags using temp columns to avoid overwrites.
    #    old_3→new_6, old_5→new_8, old_6→new_9, old_8→new_11
    #    Stages 1, 2, 4, 7 don't move.

    # Save old values we need to move
    conn.execute(sa.text("""
        UPDATE projects SET
            stage_9_complete  = stage_6_complete,
            stage_11_complete = stage_8_complete
    """))

    # Now stage_6 and stage_8 are free to overwrite
    conn.execute(sa.text("""
        UPDATE projects SET
            stage_6_complete = stage_3_complete,
            stage_8_complete = stage_5_complete
    """))

    # Clear the stages that are now new/empty
    conn.execute(sa.text("""
        UPDATE projects SET
            stage_3_complete  = false,
            stage_5_complete  = false,
            stage_10_complete = false
    """))

    # 3. Remap current_stage in one atomic CASE
    conn.execute(sa.text("""
        UPDATE projects SET current_stage = CASE current_stage
            WHEN 3 THEN 6
            WHEN 5 THEN 8
            WHEN 6 THEN 9
            WHEN 8 THEN 11
            ELSE current_stage
        END
    """))


def downgrade() -> None:
    conn = op.get_bind()

    # Reverse current_stage
    conn.execute(sa.text("""
        UPDATE projects SET current_stage = CASE current_stage
            WHEN 6 THEN 3
            WHEN 8 THEN 5
            WHEN 9 THEN 6
            WHEN 11 THEN 8
            ELSE current_stage
        END
    """))

    # Reverse completion flags
    conn.execute(sa.text("""
        UPDATE projects SET
            stage_3_complete = stage_6_complete,
            stage_5_complete = stage_8_complete,
            stage_6_complete = stage_9_complete,
            stage_8_complete = stage_11_complete
    """))

    op.drop_column("projects", "stage_11_complete")
    op.drop_column("projects", "stage_10_complete")
    op.drop_column("projects", "stage_9_complete")
