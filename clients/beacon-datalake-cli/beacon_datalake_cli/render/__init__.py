"""Rendering helpers: turn pyarrow tables and Beacon JSON into rich views.

Split across :mod:`results` (query result sets) and :mod:`metadata` (catalog
views); both are re-exported here so callers can simply ``from .render import X``.
"""

from __future__ import annotations

from .metadata import (
    datasets_to_rich,
    functions_to_rich,
    schema_to_rich,
    tables_detail_to_rich,
    tables_to_rich,
)
from .results import (
    DEFAULT_MAX_ROWS,
    footer,
    table_to_json,
    table_to_records,
    table_to_rich,
    truncation_note,
)

__all__ = [
    "DEFAULT_MAX_ROWS",
    "datasets_to_rich",
    "footer",
    "functions_to_rich",
    "schema_to_rich",
    "table_to_json",
    "table_to_records",
    "table_to_rich",
    "tables_detail_to_rich",
    "tables_to_rich",
    "truncation_note",
]
