"""
cerulean/models/__init__.py
─────────────────────────────────────────────────────────────────────────────
All SQLAlchemy ORM models. Import from here to ensure Alembic autogenerate
picks up all tables via the shared Base metadata.

Models:
    Project        — top-level migration project
    MARCFile       — uploaded MARC / CSV file
    FieldMap       — source→target field mapping (manual / AI / template)
    MapTemplate    — saved reusable mapping set (project or global scope)
    TransformManifest — tracks transform/merge pipeline runs
    DedupRule      — deduplication rule configuration
    DedupCluster   — detected duplicate group (written by dedup scan)
    ReconciliationRule — item data reconciliation rule (Stage 5)
    ReconciliationScanResult — extracted item values per vocab category
    PatronFile     — uploaded patron data file (Stage 6)
    PatronColumnMap — patron column→Koha header mapping (Stage 6)
    PatronValueRule — patron controlled list reconciliation rule (Stage 6)
    PatronScanResult — extracted patron categorical values (Stage 6)
    PushManifest   — tracks a push-to-Koha pipeline run
    SandboxInstance — KTD sandbox container lifecycle
    AuditEvent     — append-only project event log
    Suggestion     — engineer feedback / feature request
    SuggestionVote — upvote join table
    QualityScanResult — MARC data quality issue (Stage 3)
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from cerulean.core.database import Base

# ─── helpers ──────────────────────────────────────────────────────────────

def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.utcnow()


# ══════════════════════════════════════════════════════════════════════════
# PROJECT
# ══════════════════════════════════════════════════════════════════════════

class Project(Base):
    """Top-level migration project. One project = one library migration."""

    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    library_name: Mapped[str] = mapped_column(String(300), nullable=False)

    # Source ILS
    source_ils: Mapped[str | None] = mapped_column(String(100))
    ils_confidence: Mapped[float | None] = mapped_column(Float)

    # Target Koha
    koha_url: Mapped[str | None] = mapped_column(String(500))
    koha_token_enc: Mapped[str | None] = mapped_column(Text)   # Fernet encrypted
    koha_auth_type: Mapped[str] = mapped_column(String(20), default="basic")  # "basic" | "bearer"
    koha_version: Mapped[str | None] = mapped_column(String(20))
    search_engine: Mapped[str | None] = mapped_column(String(20))  # "es8" | "zebra"

    # Pipeline stage tracking (1–11, null = not started)
    current_stage: Mapped[int] = mapped_column(Integer, default=1)
    stage_1_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # MARC Ingest
    stage_2_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # ILS Detection + Config
    stage_3_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # MARC Data Quality
    stage_4_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # Versioned Run — Bibs
    stage_5_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # Field Mapping
    stage_6_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # Transform
    stage_7_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # Load (Push)
    stage_8_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # Item + Bib Reconciliation
    stage_9_complete: Mapped[bool] = mapped_column(Boolean, default=False)   # Patron Data Quality
    stage_10_complete: Mapped[bool] = mapped_column(Boolean, default=False)  # Versioned Run — Patrons
    stage_11_complete: Mapped[bool] = mapped_column(Boolean, default=False)  # Holds & Remaining Data
    stage_12_complete: Mapped[bool] = mapped_column(Boolean, default=False)  # Aspen Discovery

    # Target Aspen Discovery
    aspen_url: Mapped[str | None] = mapped_column(String(500))  # e.g. http://aspen:85

    # Target Evergreen ILS
    evergreen_db_host: Mapped[str | None] = mapped_column(String(500))
    evergreen_db_port: Mapped[int | None] = mapped_column(Integer, default=5432)
    evergreen_db_name: Mapped[str | None] = mapped_column(String(100), default="evergreen")
    evergreen_db_user: Mapped[str | None] = mapped_column(String(100))
    evergreen_db_password: Mapped[str | None] = mapped_column(Text)  # TODO: encrypt
    stage_13_complete: Mapped[bool] = mapped_column(Boolean, default=False)  # Evergreen

    # Stage 8 — Reconciliation
    reconcile_source_file: Mapped[str | None] = mapped_column(String(500))
    reconcile_scan_task_id: Mapped[str | None] = mapped_column(String(200))
    reconcile_apply_task_id: Mapped[str | None] = mapped_column(String(200))

    # Stage 2 — Migration config
    item_structure: Mapped[str | None] = mapped_column(String(20))  # "embedded" | "separate" | "none"

    # Stage 5/6 — Item CSV match config
    items_csv_match_tag: Mapped[str | None] = mapped_column(String(10))   # MARC tag, e.g. "001"
    items_csv_key_column: Mapped[str | None] = mapped_column(String(200)) # CSV column name

    # Stage 9 — Patron Data Transformation
    patron_scan_task_id: Mapped[str | None] = mapped_column(String(200))
    patron_apply_task_id: Mapped[str | None] = mapped_column(String(200))
    patron_ai_task_id: Mapped[str | None] = mapped_column(String(200))

    # Record counts (updated after each stage)
    bib_count_ingested: Mapped[int | None] = mapped_column(Integer)
    bib_count_post_dedup: Mapped[int | None] = mapped_column(Integer)
    bib_count_pushed: Mapped[int | None] = mapped_column(Integer)
    patron_count: Mapped[int | None] = mapped_column(Integer)
    hold_count: Mapped[int | None] = mapped_column(Integer)

    # Archive
    archived: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    # Relationships
    files: Mapped[list["MARCFile"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    maps: Mapped[list["FieldMap"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    dedup_rules: Mapped[list["DedupRule"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    reconciliation_rules: Mapped[list["ReconciliationRule"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    reconciliation_scan_results: Mapped[list["ReconciliationScanResult"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    patron_files: Mapped[list["PatronFile"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    item_column_maps: Mapped[list["ItemColumnMap"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    patron_column_maps: Mapped[list["PatronColumnMap"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    patron_value_rules: Mapped[list["PatronValueRule"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    patron_scan_results: Mapped[list["PatronScanResult"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    audit_events: Mapped[list["AuditEvent"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    transform_manifests: Mapped[list["TransformManifest"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    push_manifests: Mapped[list["PushManifest"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    sandbox_instances: Mapped[list["SandboxInstance"]] = relationship(back_populates="project", cascade="all, delete-orphan")


# ══════════════════════════════════════════════════════════════════════════
# MARC FILE
# ══════════════════════════════════════════════════════════════════════════

class MARCFile(Base):
    """An uploaded MARC or CSV file belonging to a project."""

    __tablename__ = "marc_files"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    filename: Mapped[str] = mapped_column(String(300), nullable=False)
    file_format: Mapped[str] = mapped_column(String(20))   # "iso2709" | "mrk" | "csv"
    file_category: Mapped[str] = mapped_column(String(20), default="marc")  # "marc" | "items_csv"
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    storage_path: Mapped[str] = mapped_column(Text)        # absolute path or S3 key

    record_count: Mapped[int | None] = mapped_column(Integer)
    ils_signal: Mapped[str | None] = mapped_column(String(100))  # detected ILS for this file

    # Tag frequency histogram — stored as JSONB: {"001": 42118, "245": 42118, ...}
    tag_frequency: Mapped[dict | None] = mapped_column(JSONB)

    # Subfield frequency — {"245": {"a": 6182, "b": 3421}, ...}
    # For items_csv: {"col": {"unique": N, "samples": [...]}}
    subfield_frequency: Mapped[dict | None] = mapped_column(JSONB)

    # CSV column headers (items_csv files only)
    column_headers: Mapped[list | None] = mapped_column(JSONB)

    # Status: "uploaded" | "indexing" | "indexed" | "error"
    status: Mapped[str] = mapped_column(String(20), default="uploaded")
    error_message: Mapped[str | None] = mapped_column(Text)

    sort_order: Mapped[int] = mapped_column(Integer, default=0)  # merge queue position

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="files")


# ══════════════════════════════════════════════════════════════════════════
# FIELD MAP
# ══════════════════════════════════════════════════════════════════════════

class FieldMap(Base):
    """
    A single source→target MARC field mapping for a project.

    Source label indicates origin:
        "manual"          — created or last edited by a human engineer
        "ai"              — created by AI suggestion, not yet edited
        "template:{id}"   — loaded from a MapTemplate
    """

    __tablename__ = "field_maps"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    # Source field
    source_tag: Mapped[str] = mapped_column(String(10), nullable=False)   # e.g. "852"
    source_sub: Mapped[str | None] = mapped_column(String(5))             # e.g. "$h"

    # Target field
    target_tag: Mapped[str] = mapped_column(String(10), nullable=False)   # e.g. "952"
    target_sub: Mapped[str | None] = mapped_column(String(5))             # e.g. "$o"

    # Transform
    # "copy" | "regex" | "lookup" | "const" | "fn" | "preset"
    transform_type: Mapped[str] = mapped_column(String(20), default="copy")
    # Expression, regex pattern, lookup table name, constant value, or Python lambda string
    transform_fn: Mapped[str | None] = mapped_column(Text)
    # Preset transform key (e.g. "date_mdy_to_dmy") — used when transform_type="preset"
    preset_key: Mapped[str | None] = mapped_column(String(50), nullable=True)
    # True = move (delete source field after copy), False = copy (keep original)
    delete_source: Mapped[bool] = mapped_column(Boolean, default=False)

    # Origin
    ai_suggested: Mapped[bool] = mapped_column(Boolean, default=False)
    ai_confidence: Mapped[float | None] = mapped_column(Float)   # 0.0–1.0
    ai_reasoning: Mapped[str | None] = mapped_column(Text)        # Claude explanation
    source_label: Mapped[str] = mapped_column(String(50), default="manual")

    # Approval
    approved: Mapped[bool] = mapped_column(Boolean, default=False)

    notes: Mapped[str | None] = mapped_column(Text)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="maps")


# ══════════════════════════════════════════════════════════════════════════
# MAP TEMPLATE
# ══════════════════════════════════════════════════════════════════════════

class MapTemplate(Base):
    """
    A saved, named set of FieldMaps that can be reused across projects.

    Scope:
        "project"  — visible only to one project (project_id must be set)
        "global"   — shared across all BWS engineers and projects
    """

    __tablename__ = "map_templates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    version: Mapped[str] = mapped_column(String(20), default="1.0")
    description: Mapped[str | None] = mapped_column(Text)

    # Scope
    scope: Mapped[str] = mapped_column(String(20), nullable=False)  # "project" | "global"
    project_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("projects.id"))

    # Metadata
    source_ils: Mapped[str | None] = mapped_column(String(100))  # e.g. "SirsiDynix Symphony"
    ai_generated: Mapped[bool] = mapped_column(Boolean, default=False)
    reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    use_count: Mapped[int] = mapped_column(Integer, default=0)

    # The maps themselves — serialised list of FieldMap-like dicts
    # [{source_tag, source_sub, target_tag, target_sub, transform_type, transform_fn, notes}]
    maps: Mapped[list | None] = mapped_column(JSONB)

    created_by: Mapped[str | None] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)


# ══════════════════════════════════════════════════════════════════════════
# TRANSFORM MANIFEST
# ══════════════════════════════════════════════════════════════════════════

class TransformManifest(Base):
    """
    Tracks a transform or merge pipeline run.

    Created by the /transform/start or /transform/merge endpoint,
    updated by the Celery task on completion or failure.
    """

    __tablename__ = "transform_manifests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    task_type: Mapped[str] = mapped_column(String(20), nullable=False)  # "transform" | "merge"
    status: Mapped[str] = mapped_column(String(20), default="running")  # "running" | "complete" | "error"
    celery_task_id: Mapped[str | None] = mapped_column(String(200))

    # Results (populated on completion)
    output_path: Mapped[str | None] = mapped_column(Text)
    files_processed: Mapped[int | None] = mapped_column(Integer)
    total_records: Mapped[int | None] = mapped_column(Integer)
    records_skipped: Mapped[int | None] = mapped_column(Integer)
    items_joined: Mapped[int | None] = mapped_column(Integer)
    duplicate_001s: Mapped[list | None] = mapped_column(JSONB)   # [{value, count}]
    file_ids: Mapped[list | None] = mapped_column(JSONB)         # source file UUIDs
    items_csv_path: Mapped[str | None] = mapped_column(Text)

    error_message: Mapped[str | None] = mapped_column(Text)

    started_at: Mapped[datetime | None] = mapped_column(DateTime, default=_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    project: Mapped["Project"] = relationship(back_populates="transform_manifests")


# ══════════════════════════════════════════════════════════════════════════
# DEDUP RULE
# ══════════════════════════════════════════════════════════════════════════

class DedupRule(Base):
    """Deduplication rule for a project. Only one rule is active at a time."""

    __tablename__ = "dedup_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    is_preset: Mapped[bool] = mapped_column(Boolean, default=False)
    # "001" | "isbn" | "title_author" | "oclc" | "custom"
    preset_key: Mapped[str | None] = mapped_column(String(50))

    # List of match fields: [{tag, sub, match_type: "exact"|"normalised", normalize: bool}]
    fields: Mapped[list | None] = mapped_column(JSONB)

    # "keep_first" | "keep_most_fields" | "keep_most_items" | "write_exceptions" | "merge_holdings"
    on_duplicate: Mapped[str] = mapped_column(String(50), default="keep_first")

    active: Mapped[bool] = mapped_column(Boolean, default=False)

    # Results from last scan
    last_scan_clusters: Mapped[int | None] = mapped_column(Integer)
    last_scan_affected: Mapped[int | None] = mapped_column(Integer)
    last_scan_at: Mapped[datetime | None] = mapped_column(DateTime)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    project: Mapped["Project"] = relationship(back_populates="dedup_rules")
    clusters: Mapped[list["DedupCluster"]] = relationship(back_populates="rule", cascade="all, delete-orphan")


class DedupCluster(Base):
    """A group of records identified as duplicates by a DedupRule scan."""

    __tablename__ = "dedup_clusters"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    rule_id: Mapped[str] = mapped_column(String(36), ForeignKey("dedup_rules.id"), nullable=False)

    # Sorted list of record identifiers: [{file_id, record_index, marc_001, field_count, item_count}]
    records: Mapped[list] = mapped_column(JSONB, nullable=False)
    primary_index: Mapped[int] = mapped_column(Integer, default=0)  # index into records[] of chosen primary

    match_key: Mapped[str | None] = mapped_column(String(200))  # the value that matched (e.g. the 001 value)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    rule: Mapped["DedupRule"] = relationship(back_populates="clusters")


# ══════════════════════════════════════════════════════════════════════════
# RECONCILIATION RULE
# ══════════════════════════════════════════════════════════════════════════

class ReconciliationRule(Base):
    """Item data reconciliation rule for a project (Stage 5).

    Maps source item-level MARC 952 subfield values to Koha controlled
    vocabulary lists. Supports rename, merge, split, and delete operations.
    """

    __tablename__ = "reconciliation_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    vocab_category: Mapped[str] = mapped_column(String(50), nullable=False)
    marc_tag: Mapped[str] = mapped_column(String(10), nullable=False)
    marc_subfield: Mapped[str] = mapped_column(String(5), nullable=False)

    # "rename" | "merge" | "split" | "delete"
    operation: Mapped[str] = mapped_column(String(20), nullable=False)

    # Source values this rule matches — list of strings
    source_values: Mapped[list] = mapped_column(JSONB, nullable=False)

    # Target value for rename/merge operations
    target_value: Mapped[str | None] = mapped_column(String(200))

    # For split operations: list of condition objects
    # [{field_check: {tag, sub, value}, target_value}, ..., {default: true, target_value}]
    split_conditions: Mapped[list | None] = mapped_column(JSONB)

    # For delete: "subfield" (remove just the subfield) or "field" (remove entire 952)
    delete_mode: Mapped[str | None] = mapped_column(String(20))

    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="reconciliation_rules")


class ReconciliationScanResult(Base):
    """Extracted item-level value from a reconciliation scan.

    One row per (project, vocab_category, source_value) with count of
    records containing that value.
    """

    __tablename__ = "reconciliation_scan_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    vocab_category: Mapped[str] = mapped_column(String(50), nullable=False)
    source_value: Mapped[str] = mapped_column(Text, nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    project: Mapped["Project"] = relationship(back_populates="reconciliation_scan_results")


# ══════════════════════════════════════════════════════════════════════════
# ITEM COLUMN MAP
# ══════════════════════════════════════════════════════════════════════════

class ItemColumnMap(Base):
    """
    CSV column → Koha 952 subfield mapping for item CSV files.

    Mirrors the PatronColumnMap pattern for item/holdings data.
    """

    __tablename__ = "item_column_maps"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    source_column: Mapped[str] = mapped_column(String(200), nullable=False)
    target_subfield: Mapped[str | None] = mapped_column(String(5))  # 952 subfield code: "a", "o", "p", etc.
    ignored: Mapped[bool] = mapped_column(Boolean, default=False)

    # Transform
    transform_type: Mapped[str | None] = mapped_column(String(20))  # copy|regex|const|lookup
    transform_config: Mapped[dict | None] = mapped_column(JSONB)

    # Approval
    approved: Mapped[bool] = mapped_column(Boolean, default=False)

    # AI origin
    ai_suggested: Mapped[bool] = mapped_column(Boolean, default=False)
    ai_confidence: Mapped[float | None] = mapped_column(Float)
    ai_reasoning: Mapped[str | None] = mapped_column(Text)
    source_label: Mapped[str] = mapped_column(String(50), default="manual")

    sort_order: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="item_column_maps")


# ══════════════════════════════════════════════════════════════════════════
# PATRON FILE
# ══════════════════════════════════════════════════════════════════════════

class PatronFile(Base):
    """An uploaded patron data file belonging to a project (Stage 6)."""

    __tablename__ = "patron_files"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    filename: Mapped[str] = mapped_column(String(300), nullable=False)
    file_format: Mapped[str | None] = mapped_column(String(20))  # csv|tsv|patron_marc|xlsx|xls|xml|fixed
    detected_format: Mapped[str | None] = mapped_column(String(20))
    parse_settings: Mapped[dict | None] = mapped_column(JSONB)
    row_count: Mapped[int | None] = mapped_column(Integer)
    column_headers: Mapped[list | None] = mapped_column(JSONB)

    # "uploaded" | "parsing" | "parsed" | "error"
    status: Mapped[str] = mapped_column(String(20), default="uploaded")

    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    project: Mapped["Project"] = relationship(back_populates="patron_files")


# ══════════════════════════════════════════════════════════════════════════
# PATRON COLUMN MAP
# ══════════════════════════════════════════════════════════════════════════

class PatronColumnMap(Base):
    """
    Legacy patron column → Koha borrower header mapping (Stage 6).

    Mirrors the FieldMap pattern for tabular patron data.
    """

    __tablename__ = "patron_column_maps"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    source_column: Mapped[str] = mapped_column(String(200), nullable=False)
    target_header: Mapped[str | None] = mapped_column(String(200))
    ignored: Mapped[bool] = mapped_column(Boolean, default=False)

    # Transform
    transform_type: Mapped[str | None] = mapped_column(String(20))  # copy|date_*|case_*|const|regex
    transform_config: Mapped[dict | None] = mapped_column(JSONB)

    # Controlled list flag
    is_controlled_list: Mapped[bool] = mapped_column(Boolean, default=False)

    # Approval
    approved: Mapped[bool] = mapped_column(Boolean, default=False)

    # AI origin
    ai_suggested: Mapped[bool] = mapped_column(Boolean, default=False)
    ai_confidence: Mapped[float | None] = mapped_column(Float)
    ai_reasoning: Mapped[str | None] = mapped_column(Text)
    source_label: Mapped[str] = mapped_column(String(50), default="manual")

    sort_order: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="patron_column_maps")


# ══════════════════════════════════════════════════════════════════════════
# PATRON VALUE RULE
# ══════════════════════════════════════════════════════════════════════════

class PatronValueRule(Base):
    """Patron controlled list reconciliation rule (Stage 6).

    Maps source patron categorical values (categorycode, branchcode, title,
    lost) to Koha controlled vocabulary. Supports rename, merge, split, and
    delete operations.
    """

    __tablename__ = "patron_value_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    koha_header: Mapped[str] = mapped_column(String(50), nullable=False)

    # "rename" | "merge" | "split" | "delete"
    operation: Mapped[str] = mapped_column(String(20), nullable=False)

    # Source values this rule matches — list of strings
    source_values: Mapped[list] = mapped_column(JSONB, nullable=False)

    # Target value for rename/merge operations
    target_value: Mapped[str | None] = mapped_column(String(200))

    # For split operations: list of condition objects
    split_conditions: Mapped[list | None] = mapped_column(JSONB)

    # For delete: "exclude_row" (remove entire patron) or "blank_field" (clear the field)
    delete_mode: Mapped[str | None] = mapped_column(String(20))

    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="patron_value_rules")


# ══════════════════════════════════════════════════════════════════════════
# PATRON SCAN RESULT
# ══════════════════════════════════════════════════════════════════════════

class PatronScanResult(Base):
    """Extracted patron categorical value from a scan (Stage 6).

    One row per (project, koha_header, source_value) with count of
    patron records containing that value.
    """

    __tablename__ = "patron_scan_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    koha_header: Mapped[str] = mapped_column(String(50), nullable=False)
    source_value: Mapped[str] = mapped_column(Text, nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    project: Mapped["Project"] = relationship(back_populates="patron_scan_results")


# ══════════════════════════════════════════════════════════════════════════
# PUSH MANIFEST
# ══════════════════════════════════════════════════════════════════════════

class PushManifest(Base):
    """Tracks a push-to-Koha pipeline run (preflight, bulkmarc, patrons, holds, circ, reindex)."""

    __tablename__ = "push_manifests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    # "preflight" | "bulkmarc" | "patrons" | "holds" | "circ" | "reindex"
    task_type: Mapped[str] = mapped_column(String(20), nullable=False)
    # "running" | "complete" | "error"
    status: Mapped[str] = mapped_column(String(20), default="running")
    celery_task_id: Mapped[str | None] = mapped_column(String(200))
    dry_run: Mapped[bool] = mapped_column(Boolean, default=True)

    records_total: Mapped[int | None] = mapped_column(Integer)
    records_success: Mapped[int | None] = mapped_column(Integer)
    records_failed: Mapped[int | None] = mapped_column(Integer)

    error_message: Mapped[str | None] = mapped_column(Text)
    result_data: Mapped[dict | None] = mapped_column(JSONB)

    started_at: Mapped[datetime | None] = mapped_column(DateTime, default=_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    project: Mapped["Project"] = relationship(back_populates="push_manifests")


# ══════════════════════════════════════════════════════════════════════════
# SANDBOX INSTANCE
# ══════════════════════════════════════════════════════════════════════════

class SandboxInstance(Base):
    """KTD sandbox container lifecycle tracker."""

    __tablename__ = "sandbox_instances"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    container_id: Mapped[str | None] = mapped_column(String(100))
    container_name: Mapped[str | None] = mapped_column(String(200))

    # "provisioning" | "running" | "stopping" | "stopped" | "error"
    status: Mapped[str] = mapped_column(String(20), default="provisioning")

    koha_url: Mapped[str | None] = mapped_column(String(500))
    exposed_port: Mapped[int | None] = mapped_column(Integer)
    celery_task_id: Mapped[str | None] = mapped_column(String(200))
    error_message: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    project: Mapped["Project"] = relationship(back_populates="sandbox_instances")


# ══════════════════════════════════════════════════════════════════════════
# AUDIT EVENT
# ══════════════════════════════════════════════════════════════════════════

class AuditEvent(Base):
    """
    Append-only log of every significant event on a project.

    Never delete rows from this table.
    Written by: API endpoints (user decisions), Celery tasks (system events).
    Streamed to the frontend via SSE during active pipeline runs.
    """

    __tablename__ = "audit_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False)

    stage: Mapped[int | None] = mapped_column(Integer)   # 1–8; null for cross-project events
    # "info" | "warn" | "error" | "complete"
    level: Mapped[str] = mapped_column(String(10), nullable=False)
    # Tag shown in the log UI — e.g. "[ingest]", "[ai-map]", "[push]"
    tag: Mapped[str | None] = mapped_column(String(30))
    message: Mapped[str] = mapped_column(Text, nullable=False)

    # Optional context — record-specific events
    record_001: Mapped[str | None] = mapped_column(String(100))
    extra: Mapped[dict | None] = mapped_column(JSONB)  # arbitrary structured context

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    project: Mapped["Project"] = relationship(back_populates="audit_events")


# ══════════════════════════════════════════════════════════════════════════
# SUGGESTIONS & FEATURE FEEDBACK
# ══════════════════════════════════════════════════════════════════════════

class Suggestion(Base):
    """Engineer-submitted bug report, feature request, workflow idea, or discussion."""

    __tablename__ = "suggestions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # "feature" | "bug" | "workflow" | "discussion"
    type: Mapped[str] = mapped_column(String(20), nullable=False)
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)

    # Optional project link
    project_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("projects.id"))

    # "open" | "confirmed" | "in_progress" | "shipped" | "closed"
    status: Mapped[str] = mapped_column(String(20), default="open")

    submitted_by: Mapped[str | None] = mapped_column(String(200))
    vote_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    votes: Mapped[list["SuggestionVote"]] = relationship(back_populates="suggestion", cascade="all, delete-orphan")


class SuggestionVote(Base):
    """One upvote per user per suggestion."""

    __tablename__ = "suggestion_votes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    suggestion_id: Mapped[str] = mapped_column(String(36), ForeignKey("suggestions.id"), nullable=False)
    user_email: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    suggestion: Mapped["Suggestion"] = relationship(back_populates="votes")


# ══════════════════════════════════════════════════════════════════════════
# STAGE 3 — MARC DATA QUALITY
# ══════════════════════════════════════════════════════════════════════════


class QualityScanResult(Base):
    """One quality issue found during a Stage 3 data quality scan.

    Categories: encoding, diacritics, indicators, subfield_structure,
                field_length, leader_008, duplicates, blank_fields
    """

    __tablename__ = "quality_scan_results"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False, index=True)
    scan_type: Mapped[str] = mapped_column(String(20), nullable=False)       # "bib" | "item"
    category: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)        # "error" | "warning" | "info"
    record_index: Mapped[int] = mapped_column(Integer, nullable=False)       # 0-based record position
    file_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("marc_files.id"))
    tag: Mapped[str | None] = mapped_column(String(5))                       # affected MARC tag
    subfield: Mapped[str | None] = mapped_column(String(5))                  # affected subfield code
    description: Mapped[str] = mapped_column(Text, nullable=False)           # human-readable issue
    original_value: Mapped[str | None] = mapped_column(Text)                 # the problematic value
    suggested_fix: Mapped[str | None] = mapped_column(Text)                  # auto-fix value if available
    status: Mapped[str] = mapped_column(String(20), default="unresolved")    # unresolved|auto_fixed|manually_fixed|ignored
    fixed_value: Mapped[str | None] = mapped_column(Text)
    fixed_by: Mapped[str | None] = mapped_column(String(200))
    fixed_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)


# ══════════════════════════════════════════════════════════════════════════
# STAGE 4 / 10 — VERSIONED MIGRATION RUNS
# ══════════════════════════════════════════════════════════════════════════


class MigrationVersion(Base):
    """Immutable snapshot of a migration run for a specific data type.

    data_type: "bib" (Stage 4), "item" (Stage 8), "patron" (Stage 10), "holds" (Stage 11)
    """

    __tablename__ = "migration_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"), nullable=False, index=True)
    data_type: Mapped[str] = mapped_column(String(20), nullable=False)       # bib|item|patron|holds
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str | None] = mapped_column(String(200))                   # optional user label
    snapshot_path: Mapped[str] = mapped_column(Text, nullable=False)         # path to snapshot file
    mapping_snapshot: Mapped[dict | None] = mapped_column(JSONB)             # field maps at run time
    quality_log: Mapped[dict | None] = mapped_column(JSONB)                  # Stage 3 remediation summary
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    run_duration_seconds: Mapped[int | None] = mapped_column(Integer)
    load_success: Mapped[int | None] = mapped_column(Integer)
    load_failed: Mapped[int | None] = mapped_column(Integer)
    created_by: Mapped[str | None] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
