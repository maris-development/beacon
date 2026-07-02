"""Tests for REPL meta-command helpers."""

from __future__ import annotations

from beacon_cli.repl.meta import split_sql_script


def test_split_sql_script_separates_statements():
    assert split_sql_script("SELECT 1;\nSELECT 2 ;") == ["SELECT 1", "SELECT 2"]


def test_split_sql_script_skips_blank_pieces():
    assert split_sql_script("  ;  \n\n;") == []


def test_split_sql_script_ignores_semicolons_in_strings():
    assert split_sql_script("SELECT ';' AS s") == ["SELECT ';' AS s"]


def test_split_sql_script_keeps_multiline_statements():
    out = split_sql_script("SELECT *\nFROM obs\nWHERE x = 1;")
    assert out == ["SELECT *\nFROM obs\nWHERE x = 1"]


def test_split_sql_script_drops_comment_only_pieces():
    out = split_sql_script("SELECT 1;\n-- a trailing note\n")
    assert out == ["SELECT 1"]
