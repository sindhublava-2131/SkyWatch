"""Canonical data schema loader and record validator for SkyWatch."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_CACHED_SCHEMAS: Optional[Dict[str, Any]] = None


def load_schemas(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and cache schemas definition from schemas.yaml."""
    global _CACHED_SCHEMAS
    if config_path is None and _CACHED_SCHEMAS is not None:
        return _CACHED_SCHEMAS

    schema_file = Path(config_path) if config_path else Path("config/schemas.yaml")
    if not schema_file.is_file():
        raise FileNotFoundError(f"Schema configuration file not found: {schema_file}")

    with open(schema_file, "r", encoding="utf-8") as f:
        schemas = yaml.safe_load(f)

    if config_path is None:
        _CACHED_SCHEMAS = schemas
    return schemas


def get_schema(schema_name: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve schema dictionary by stream/entity name."""
    schemas = load_schemas(config_path)
    if schema_name not in schemas:
        raise KeyError(f"Unknown schema name '{schema_name}'. Available: {list(schemas.keys())}")
    return schemas[schema_name]


def get_column_names(schema_name: str, config_path: Optional[str] = None) -> List[str]:
    """Return ordered list of column names for a schema."""
    schema = get_schema(schema_name, config_path)
    columns = schema.get("columns", [])
    return [col["name"] for col in columns]


def validate_record_dict(
    record: Dict[str, Any],
    schema_name: str,
    config_path: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """Validate a dictionary against the named schema."""
    schema = get_schema(schema_name, config_path)
    columns = schema.get("columns", [])

    for col in columns:
        col_name = col["name"]
        col_type = col.get("type", "string")
        nullable = col.get("nullable", True)

        if col_name not in record:
            if not nullable:
                return False, f"Missing non-nullable column '{col_name}'"
            continue

        val = record[col_name]
        if val is None:
            if not nullable:
                return False, f"Column '{col_name}' cannot be null"
            continue

        # Type checks
        if col_type in ("double", "float"):
            if not isinstance(val, (int, float)):
                try:
                    float(val)
                except (ValueError, TypeError):
                    return False, f"Column '{col_name}' must be numeric (double), got {type(val)}"
        elif col_type == "integer":
            if isinstance(val, bool):
                return False, f"Column '{col_name}' must be integer, got bool"
            if not isinstance(val, int):
                try:
                    int(val)
                except (ValueError, TypeError):
                    return False, f"Column '{col_name}' must be integer, got {type(val)}"
        elif col_type == "boolean":
            if not isinstance(val, bool):
                return False, f"Column '{col_name}' must be boolean, got {type(val)}"
        elif col_type == "string":
            if not isinstance(val, str):
                return False, f"Column '{col_name}' must be string, got {type(val)}"

    return True, None
