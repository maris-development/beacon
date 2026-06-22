"""Tests for catalog metadata rendering."""

from __future__ import annotations

from rich.console import Console

from beacon_cli.render.metadata import friendly_kind, tables_detail_to_rich


def _text(renderable) -> str:
    console = Console(width=200, no_color=True, record=True)
    console.print(renderable)
    return console.export_text()


def test_tables_detail_renders_kind_format_location():
    entries = [
        ("wod", {"definition_type": "listing_table", "file_type": "NC", "location": "wod.nc"}),
        ("default", {}),  # no config -> blank metadata
    ]
    out = _text(tables_detail_to_rich(entries))
    # raw "listing_table" is mapped to a friendly label
    assert "external (files)" in out and "listing_table" not in out
    assert "wod" in out and "NC" in out and "wod.nc" in out
    assert "default" in out  # still listed even with empty config


def test_friendly_kind_maps_known_and_passes_through_unknown():
    assert friendly_kind("listing_table") == "external (files)"
    assert friendly_kind("remote_table") == "external (remote)"
    assert friendly_kind("materialized_view") == "materialized view"
    assert friendly_kind("something_new") == "something_new"  # unknown -> unchanged
    assert friendly_kind("") == ""
