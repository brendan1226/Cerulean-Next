"""Add indexes on project_id foreign keys across all tables.

Every table with a project_id FK was missing an index, causing full
table scans on the most common query pattern (filter by project).
Also adds composite indexes on (project_id, created_at) for audit_events
and (project_id, status) for transform_manifests and push_manifests.

Revision ID: z6t1u2v3w4x5
Revises: y5s0t1u2v3w4
Create Date: 2026-04-17
"""
from alembic import op

revision = "z6t1u2v3w4x5"
down_revision = "y5s0t1u2v3w4"
branch_labels = None
depends_on = None

_SIMPLE_INDEXES = [
    ("ix_marc_files_project", "marc_files", ["project_id"]),
    ("ix_field_maps_project", "field_maps", ["project_id"]),
    ("ix_map_templates_project", "map_templates", ["project_id"]),
    ("ix_transform_manifests_project", "transform_manifests", ["project_id"]),
    ("ix_dedup_rules_project", "dedup_rules", ["project_id"]),
    ("ix_dedup_clusters_rule", "dedup_clusters", ["rule_id"]),
    ("ix_reconciliation_rules_project", "reconciliation_rules", ["project_id"]),
    ("ix_reconciliation_scan_results_project", "reconciliation_scan_results", ["project_id"]),
    ("ix_item_column_maps_project", "item_column_maps", ["project_id"]),
    ("ix_patron_files_project", "patron_files", ["project_id"]),
    ("ix_patron_column_maps_project", "patron_column_maps", ["project_id"]),
    ("ix_patron_value_rules_project", "patron_value_rules", ["project_id"]),
    ("ix_patron_scan_results_project", "patron_scan_results", ["project_id"]),
    ("ix_push_manifests_project", "push_manifests", ["project_id"]),
    ("ix_sandbox_instances_project", "sandbox_instances", ["project_id"]),
    ("ix_audit_events_project", "audit_events", ["project_id"]),
    ("ix_holds_files_project", "holds_files", ["project_id"]),
    ("ix_quality_scan_results_file", "quality_scan_results", ["file_id"]),
    ("ix_macros_project", "macros", ["project_id"]),
    ("ix_suggestions_project", "suggestions", ["project_id"]),
]

_COMPOSITE_INDEXES = [
    ("ix_audit_events_project_created", "audit_events", ["project_id", "created_at"]),
    ("ix_transform_manifests_project_status", "transform_manifests", ["project_id", "status"]),
    ("ix_push_manifests_project_status", "push_manifests", ["project_id", "status"]),
    ("ix_field_maps_project_approved", "field_maps", ["project_id", "approved"]),
]


def upgrade() -> None:
    for name, table, cols in _SIMPLE_INDEXES + _COMPOSITE_INDEXES:
        op.create_index(name, table, cols, if_not_exists=True)


def downgrade() -> None:
    for name, table, cols in reversed(_SIMPLE_INDEXES + _COMPOSITE_INDEXES):
        op.drop_index(name, table_name=table, if_exists=True)
