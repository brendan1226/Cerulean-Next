"""Unit tests for the Phase 4 transform sandbox.

Covers the three sandboxing invariants that must hold forever:

1. **Spec examples work.** cerulean_ai_spec.md §5.3 lists the exact
   expressions the AI is supposed to produce. If any of them stops
   evaluating, AI Transform Rule Generation is broken.
2. **Dangerous escapes stay blocked.** open, exec, eval of arbitrary
   builtins, imports of os/subprocess/sys/ctypes — none may succeed
   inside the sandbox.
3. **apply_fn_safe surfaces errors instead of swallowing them.** The
   UI preview depends on this to show per-row failures.
"""

import pytest

from cerulean.tasks.transform import (
    _SAFE_GLOBALS,
    _SANDBOX_ALLOWED_IMPORTS,
    _apply_fn,
    apply_fn_safe,
)


class TestSpecExamplesExecute:
    """Every example from cerulean_ai_spec.md §5.3 must produce the
    expected output. A regression here breaks the feature's demo."""

    @pytest.mark.parametrize("before,expr,expected", [
        ("Science, technology, and society.",
         r"re.sub(r'[.,]+$', '', value.strip())",
         "Science, technology, and society"),
        ("03/15/1987",
         "datetime.strptime(value.strip(), '%m/%d/%Y').strftime('%Y-%m-%d') if value.strip() else value",
         "1987-03-15"),
        ("The great gatsby / F. Scott Fitzgerald.",
         r"re.sub(r'\s*/.*$', '', value).rstrip()",
         "The great gatsby"),
        ("main", "value.upper()", "MAIN"),
    ])
    def test_spec_example(self, before, expr, expected):
        after, err = apply_fn_safe(before, expr)
        assert err is None, f"unexpected error: {err}"
        assert after == expected


class TestSandboxBlocksDangerousEscapes:
    """These must NEVER succeed. Adding a new allowed import? Add a
    corresponding blocker here. Regression-proof."""

    @pytest.mark.parametrize("expr", [
        'open("/etc/passwd").read()',
        'exec("print(1)")',
        'eval("1+1")',
        'compile("x=1", "", "exec")',
        '__import__("os").system("ls")',
        '__import__("subprocess").run(["ls"])',
        '__import__("socket")',
        '__import__("sys").modules',
        '__import__("ctypes")',
        '__import__("importlib")',
        '__import__("pickle")',
        '__import__("builtins").open',
        'globals()',
    ])
    def test_expression_is_blocked(self, expr):
        result, err = apply_fn_safe("anything", expr)
        assert err is not None, f"sandbox leak: {expr!r} executed with result {result!r}"
        # Fallback on error returns the original value verbatim
        assert result == "anything"


class TestSandboxWhitelistCoversKnownUses:
    """Value-sensitive: bumping the whitelist bump test here too."""

    def test_whitelist_contains_exactly_expected_modules(self):
        expected = {
            "re", "_sre",
            "datetime", "_datetime",
            "time", "_strptime",
            "locale", "_locale",
            "encodings",
            "string",
            "calendar",
        }
        assert _SANDBOX_ALLOWED_IMPORTS == expected, (
            "Sandbox whitelist changed — confirm the new module is safe "
            "(no file/network/process access) before expanding."
        )

    def test_re_and_datetime_names_present_in_globals(self):
        assert "re" in _SAFE_GLOBALS
        assert "datetime" in _SAFE_GLOBALS
        assert "date" in _SAFE_GLOBALS
        assert "timedelta" in _SAFE_GLOBALS
        assert "__builtins__" in _SAFE_GLOBALS


class TestApplyFnSafeErrorReporting:
    def test_zero_division_surfaces_clearly(self):
        result, err = apply_fn_safe("1", "1/0")
        assert result == "1", "value should fall back to the input on error"
        assert err is not None
        assert "ZeroDivisionError" in err

    def test_name_error_surfaces_clearly(self):
        result, err = apply_fn_safe("x", "undefined_name")
        assert err is not None
        assert "NameError" in err

    def test_empty_expression_returns_value_unchanged(self):
        result, err = apply_fn_safe("x", "")
        assert result == "x"
        assert err is None


class TestApplyFnVsApplyFnSafe:
    """_apply_fn (production path) swallows errors silently and returns
    the input unchanged — which is exactly what the transform pipeline
    wants in bulk mode. apply_fn_safe exists for previews that need the
    error message."""

    def test_apply_fn_swallows_errors(self):
        # Bad expression produces input verbatim, no exception raised
        assert _apply_fn("hello", "1/0") == "hello"
        assert _apply_fn("hello", "undefined_name") == "hello"

    def test_apply_fn_handles_none_return(self):
        # A None result should leave the value unchanged
        assert _apply_fn("hello", "None") == "hello"

    def test_apply_fn_coerces_to_string(self):
        assert _apply_fn("abc", "len(value)") == "3"
