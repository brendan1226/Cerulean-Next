"""Unit tests for cerulean/utils/sql_parser.py.

The SQL parser extracts code/description pairs from INSERT INTO statements
in control value SQL files pushed via the integration API.
"""

import pytest

from cerulean.utils.sql_parser import parse_insert_statements


class TestBranches:
    def test_basic_branch_insert(self):
        sql = "INSERT INTO branches (branchcode, branchname) VALUES ('MAIN', 'Main Library');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0] == {"cv_type": "branches", "code": "MAIN", "description": "Main Library"}

    def test_multiple_inserts(self):
        sql = """
        INSERT INTO branches (branchcode, branchname) VALUES ('MAIN', 'Main Library');
        INSERT INTO branches (branchcode, branchname) VALUES ('BR1', 'Branch One');
        """
        result = parse_insert_statements(sql)
        assert len(result) == 2
        assert result[1]["code"] == "BR1"

    def test_insert_ignore(self):
        sql = "INSERT IGNORE INTO branches (branchcode, branchname) VALUES ('MAIN', 'Main Library');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0]["code"] == "MAIN"


class TestItemTypes:
    def test_itemtype_insert(self):
        sql = "INSERT INTO itemtypes (itemtype, description) VALUES ('BK', 'Books');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0] == {"cv_type": "item_types", "code": "BK", "description": "Books"}


class TestPatronCategories:
    def test_categories_insert(self):
        sql = "INSERT INTO categories (categorycode, description) VALUES ('ADULT', 'Adult patron');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0] == {"cv_type": "patron_categories", "code": "ADULT", "description": "Adult patron"}


class TestAuthorisedValues:
    def test_loc_authorised_value(self):
        sql = "INSERT INTO authorised_values (category, authorised_value, lib) VALUES ('LOC', 'REF', 'Reference');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0] == {"cv_type": "shelving_locations", "code": "REF", "description": "Reference"}

    def test_ccode_authorised_value(self):
        sql = "INSERT INTO authorised_values (category, authorised_value, lib) VALUES ('CCODE', 'FIC', 'Fiction');"
        result = parse_insert_statements(sql)
        assert result[0]["cv_type"] == "collection_codes"

    def test_unknown_category_gets_prefixed(self):
        sql = "INSERT INTO authorised_values (category, authorised_value, lib) VALUES ('CUSTOM', 'X', 'Custom value');"
        result = parse_insert_statements(sql)
        assert result[0]["cv_type"] == "av:custom"


class TestEscaping:
    def test_escaped_single_quotes(self):
        sql = r"INSERT INTO branches (branchcode, branchname) VALUES ('OH', 'O\'Hare Library');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0]["description"] == "O'Hare Library"

    def test_backtick_table_name(self):
        sql = "INSERT INTO `branches` (`branchcode`, `branchname`) VALUES ('MAIN', 'Main');"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0]["code"] == "MAIN"

    def test_double_quoted_identifiers(self):
        sql = 'INSERT INTO "branches" ("branchcode", "branchname") VALUES (\'MAIN\', \'Main\');'
        result = parse_insert_statements(sql)
        assert len(result) == 1


class TestEdgeCases:
    def test_empty_string(self):
        assert parse_insert_statements("") == []

    def test_comments_ignored(self):
        sql = """
        -- This is a comment
        /* Block comment */
        INSERT INTO branches (branchcode, branchname) VALUES ('MAIN', 'Main Library');
        """
        result = parse_insert_statements(sql)
        assert len(result) == 1

    def test_unrecognized_table_skipped(self):
        sql = "INSERT INTO unknown_table (a, b) VALUES ('x', 'y');"
        result = parse_insert_statements(sql)
        assert result == []

    def test_column_count_mismatch_skipped(self):
        sql = "INSERT INTO branches (branchcode, branchname) VALUES ('MAIN');"
        result = parse_insert_statements(sql)
        assert result == []

    def test_mixed_tables_in_one_file(self):
        sql = """
        INSERT INTO branches (branchcode, branchname) VALUES ('MAIN', 'Main Library');
        INSERT INTO itemtypes (itemtype, description) VALUES ('BK', 'Books');
        INSERT INTO authorised_values (category, authorised_value, lib) VALUES ('LOC', 'REF', 'Reference');
        INSERT INTO categories (categorycode, description) VALUES ('ADULT', 'Adult');
        """
        result = parse_insert_statements(sql)
        assert len(result) == 4
        types = {r["cv_type"] for r in result}
        assert types == {"branches", "item_types", "shelving_locations", "patron_categories"}

    def test_numeric_values_handled(self):
        sql = "INSERT INTO categories (categorycode, description, category_type, enrolmentperiod) VALUES ('ADULT', 'Adult', 'A', 99);"
        result = parse_insert_statements(sql)
        assert len(result) == 1
        assert result[0]["code"] == "ADULT"
