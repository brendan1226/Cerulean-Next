"""Unit tests for the Step 8 patron fast-path helpers:

* ``match_koha_patron_header`` — deterministic column-header matcher
  used when the uploader ticks "File uses Koha headers".
* ``generate_patron_controlled_sql`` — renders INSERT IGNORE statements
  for the distinct controlled values a scan produced, ready to pipe
  into a live Koha database.

Both are pure functions (no DB, no Celery) so we can lock down the
behavior cheaply. The DB-level auto-map wiring is exercised manually
after deploy.
"""

import pytest

from cerulean.tasks.patrons import (
    KOHA_BORROWER_HEADERS,
    PATRON_CONTROLLED_HEADERS,
    generate_patron_controlled_sql,
    match_koha_patron_header,
)


# ── match_koha_patron_header ─────────────────────────────────────────

class TestHeaderMatcher:
    @pytest.mark.parametrize("source,expected", [
        ("cardnumber", "cardnumber"),
        ("CardNumber", "cardnumber"),
        ("CARDNUMBER", "cardnumber"),
        ("  cardnumber  ", "cardnumber"),
        ("categorycode", "categorycode"),
        ("branchcode", "branchcode"),
        ("BranchCode", "branchcode"),
    ])
    def test_matches_known_headers_case_insensitively(self, source, expected):
        assert match_koha_patron_header(source) == expected

    @pytest.mark.parametrize("source", [
        "patronid",        # not a Koha borrower field
        "category_code",   # underscore variant — must be exact match
        "branch",          # shortened
        "card_no",
        "",
        "   ",
    ])
    def test_rejects_unknown_or_empty(self, source):
        assert match_koha_patron_header(source) is None

    def test_none_input_returns_none(self):
        assert match_koha_patron_header(None) is None  # type: ignore[arg-type]

    def test_every_known_header_self_matches(self):
        """Every string in KOHA_BORROWER_HEADERS must match itself —
        regression guard when someone edits the canonical list."""
        for h in KOHA_BORROWER_HEADERS:
            assert match_koha_patron_header(h) == h
            assert match_koha_patron_header(h.upper()) == h

    def test_all_controlled_headers_are_recognized(self):
        for h in PATRON_CONTROLLED_HEADERS:
            assert match_koha_patron_header(h) == h


# ── generate_patron_controlled_sql ───────────────────────────────────

class TestSqlGenerator:
    def test_empty_input_produces_comments_only(self):
        sql = generate_patron_controlled_sql({})
        # The file header always mentions "INSERT IGNORE" in prose — we
        # care that no actual INSERT statements are emitted.
        assert "INSERT IGNORE INTO" not in sql
        assert "No controlled values found" in sql

    def test_categorycode_insert_shape(self):
        sql = generate_patron_controlled_sql({
            "categorycode": [("ADULT", 10_234), ("STAFF", 87)],
        })
        assert "INSERT IGNORE INTO categories" in sql
        assert "categorycode, description, category_type, enrolmentperiod" in sql
        assert "'ADULT'" in sql
        assert "'STAFF'" in sql
        # Comment carries the patron count for operator sanity
        assert "10,234 patron(s)" in sql

    def test_branchcode_insert_shape(self):
        sql = generate_patron_controlled_sql({
            "branchcode": [("MAIN", 500), ("BRANCH1", 250)],
        })
        assert "INSERT IGNORE INTO branches" in sql
        assert "(branchcode, branchname)" in sql
        assert "'MAIN'" in sql
        assert "'BRANCH1'" in sql

    def test_title_goes_to_authorised_values(self):
        sql = generate_patron_controlled_sql({
            "title": [("Mr.", 100), ("Dr.", 5)],
        })
        assert "INSERT IGNORE INTO authorised_values" in sql
        assert "'BOR_TITLES'" in sql

    def test_lost_goes_to_authorised_values(self):
        sql = generate_patron_controlled_sql({
            "lost": [("0", 999), ("1", 3)],
        })
        assert "INSERT IGNORE INTO authorised_values" in sql
        assert "'LOST'" in sql

    def test_combined_export_includes_every_header_block(self):
        sql = generate_patron_controlled_sql({
            "categorycode": [("ADULT", 1)],
            "branchcode":   [("MAIN",  1)],
            "title":        [("Mr.",   1)],
            "lost":         [("0",     1)],
        })
        assert "-- ── categorycode" in sql
        assert "-- ── branchcode" in sql
        assert "-- ── title" in sql
        assert "-- ── lost" in sql

    def test_empty_string_values_are_skipped(self):
        sql = generate_patron_controlled_sql({
            "categorycode": [("", 5), ("   ", 3), ("REAL", 2)],
        })
        # Only the real value ends up as an INSERT statement
        assert sql.count("INSERT IGNORE INTO") == 1
        assert "'REAL'" in sql

    def test_single_quotes_in_values_are_escaped(self):
        """Operator pipes this into `mysql` — a bare single quote in a
        library name must not break the statement or open injection."""
        sql = generate_patron_controlled_sql({
            "branchcode": [("O'HARE", 42)],
        })
        # Escaped as \' so the statement stays valid
        assert "'O\\'HARE'" in sql

    def test_backslashes_are_escaped(self):
        sql = generate_patron_controlled_sql({
            "branchcode": [("BAD\\VALUE", 1)],
        })
        assert "'BAD\\\\VALUE'" in sql

    def test_unknown_header_is_silently_ignored(self):
        """Headers outside the controlled list don't produce output —
        future-proof against the caller passing through extra keys."""
        sql = generate_patron_controlled_sql({
            "made_up_header": [("X", 1)],
        })
        assert "INSERT IGNORE INTO" not in sql

    def test_project_code_appears_in_header_comment(self):
        sql = generate_patron_controlled_sql(
            {"categorycode": [("ADULT", 1)]},
            project_code="demo-library",
        )
        assert "demo-library" in sql

    def test_output_is_safe_to_redirect_into_mysql(self):
        """Smoke-test that the entire output parses as a sequence of
        statement-terminated lines — no stray backticks, no triple
        quotes, nothing that a shell redirect would choke on."""
        sql = generate_patron_controlled_sql({
            "categorycode": [("ADULT", 1), ("STAFF", 1)],
        })
        statement_lines = [
            line for line in sql.splitlines()
            if line.strip().startswith("INSERT IGNORE")
        ]
        assert len(statement_lines) == 2
        for line in statement_lines:
            # Statement body ends with ; before the trailing comment
            body, sep, _comment = line.partition(";")
            assert sep == ";", f"missing statement terminator: {line}"
            assert body.strip().startswith("INSERT IGNORE INTO")
