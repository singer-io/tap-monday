"""Unit tests for tap_monday discovery — discover() and get_schemas().

Covers:
  1.  discover() returns a singer Catalog whose entries map 1-to-1 with STREAMS.
  2.  Every CatalogEntry carries the right stream name, tap_stream_id,
      key_properties, schema, and metadata.
  3.  Replication method and key-properties are reflected in the entry metadata.
  4.  Field-level inclusion metadata (available / automatic) is present.
  5.  A schema file exists on disk for every stream registered in STREAMS.
  6.  Every schema file is valid JSON with a root "type": "object".
  7.  Regression: array items in schemas must allow null
      ("type": ["null","object"]) — the class of bug that caused the prod
      workspaces SchemaMismatch crash.
  8.  get_schemas() returns entries for every STREAMS key.
  9.  discover() propagates exceptions raised by get_schemas().
  10. Schema properties match the corresponding CatalogEntry schema fields.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog, CatalogEntry

from tap_monday.discover import discover
from tap_monday.schema import get_schemas, get_abs_path
from tap_monday.streams import STREAMS

SCHEMAS_DIR = get_abs_path("schemas")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_all_schemas():
    """Return {stream_name: schema_dict} for every file in the schemas dir."""
    result = {}
    for stream_name in STREAMS:
        path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
        if os.path.exists(path):
            with open(path) as fh:
                result[stream_name] = json.load(fh)
    return result


def _collect_bad_items(schema, path=""):
    """
    Recursively walk *schema* and return a list of dot-paths where an array's
    items schema has "object" in its type but NOT "null".

    These are the places that will cause singer SchemaMismatch when the API
    returns a null entry inside an array (e.g. teams_subscribers: [null, ...]).
    """
    issues = []
    if not isinstance(schema, dict):
        return issues

    raw_type = schema.get("type", [])
    schema_types = [raw_type] if isinstance(raw_type, str) else raw_type

    if "array" in schema_types:
        items = schema.get("items", {})
        if items:
            raw_item_type = items.get("type", [])
            item_types = [raw_item_type] if isinstance(raw_item_type, str) else raw_item_type
            if "object" in item_types and "null" not in item_types:
                issues.append(f"{path}.items" if path else "items")
            # Recurse into item properties
            for prop, prop_schema in items.get("properties", {}).items():
                child_path = f"{path}.items.{prop}" if path else f"items.{prop}"
                issues.extend(_collect_bad_items(prop_schema, child_path))

    # Recurse into object properties
    for prop, prop_schema in schema.get("properties", {}).items():
        child_path = f"{path}.{prop}" if path else prop
        issues.extend(_collect_bad_items(prop_schema, child_path))

    return issues


# ---------------------------------------------------------------------------
# 1 – discover() structure
# ---------------------------------------------------------------------------

class TestDiscoverReturnsCatalog(unittest.TestCase):
    """discover() must return a populated singer Catalog."""

    def test_returns_catalog_instance(self):
        catalog = discover()
        self.assertIsInstance(catalog, Catalog)

    def test_catalog_has_all_streams(self):
        catalog = discover()
        catalog_stream_names = {entry.stream for entry in catalog.streams}
        self.assertEqual(catalog_stream_names, set(STREAMS.keys()))

    def test_no_duplicate_streams(self):
        catalog = discover()
        names = [entry.stream for entry in catalog.streams]
        self.assertEqual(len(names), len(set(names)),
                         "discover() produced duplicate catalog entries")


# ---------------------------------------------------------------------------
# 2 & 3 – CatalogEntry fields
# ---------------------------------------------------------------------------

class TestCatalogEntryFields(unittest.TestCase):
    """Each CatalogEntry must carry correctly-populated fields."""

    @classmethod
    def setUpClass(cls):
        cls.catalog = discover()
        cls.entries = {e.stream: e for e in cls.catalog.streams}

    def test_stream_equals_tap_stream_id(self):
        for name, entry in self.entries.items():
            with self.subTest(stream=name):
                self.assertEqual(entry.stream, entry.tap_stream_id)

    def test_key_properties_match_stream_class(self):
        for name, entry in self.entries.items():
            expected = list(STREAMS[name].key_properties)
            with self.subTest(stream=name):
                self.assertEqual(list(entry.key_properties), expected)

    def test_schema_is_not_empty(self):
        for name, entry in self.entries.items():
            with self.subTest(stream=name):
                schema_dict = entry.schema.to_dict()
                self.assertIn("properties", schema_dict,
                              f"Schema for '{name}' has no 'properties' key")
                self.assertGreater(len(schema_dict["properties"]), 0)

    def test_metadata_is_not_empty(self):
        for name, entry in self.entries.items():
            with self.subTest(stream=name):
                self.assertIsNotNone(entry.metadata)
                self.assertGreater(len(entry.metadata), 0)

    def test_replication_method_in_metadata(self):
        from singer import metadata as md
        for name, entry in self.entries.items():
            mdata_map = md.to_map(entry.metadata)
            replication_method = mdata_map.get((), {}).get("forced-replication-method")
            with self.subTest(stream=name):
                self.assertIsNotNone(
                    replication_method,
                    f"'forced-replication-method' missing in root metadata for '{name}'"
                )
                self.assertEqual(
                    replication_method.upper(),
                    STREAMS[name].replication_method.upper()
                )


# ---------------------------------------------------------------------------
# 4 – Field inclusion metadata
# ---------------------------------------------------------------------------

class TestFieldInclusionMetadata(unittest.TestCase):
    """Every schema field must have an 'inclusion' value in metadata."""

    @classmethod
    def setUpClass(cls):
        cls.catalog = discover()

    def test_all_fields_have_inclusion(self):
        from singer import metadata as md
        valid_inclusions = {"available", "automatic", "unsupported"}
        for entry in self.catalog.streams:
            mdata_map = md.to_map(entry.metadata)
            schema_props = entry.schema.to_dict().get("properties", {})
            for field_name in schema_props:
                breadcrumb = ("properties", field_name)
                inclusion = mdata_map.get(breadcrumb, {}).get("inclusion")
                with self.subTest(stream=entry.stream, field=field_name):
                    self.assertIn(
                        inclusion, valid_inclusions,
                        f"'{entry.stream}.{field_name}' has invalid inclusion '{inclusion}'"
                    )

    def test_replication_keys_are_automatic(self):
        from singer import metadata as md
        for name, stream_cls in STREAMS.items():
            if not stream_cls.replication_keys:
                continue
            catalog = discover()
            entries = {e.stream: e for e in catalog.streams}
            entry = entries[name]
            mdata_map = md.to_map(entry.metadata)
            for key in stream_cls.replication_keys:
                breadcrumb = ("properties", key)
                inclusion = mdata_map.get(breadcrumb, {}).get("inclusion")
                with self.subTest(stream=name, field=key):
                    self.assertEqual(
                        inclusion, "automatic",
                        f"Replication key '{key}' in '{name}' should be 'automatic', got '{inclusion}'"
                    )


# ---------------------------------------------------------------------------
# 5 & 6 – Schema files on disk
# ---------------------------------------------------------------------------

class TestSchemaFiles(unittest.TestCase):
    """Schema files must exist and be valid JSON with a root object type."""

    def test_schema_file_exists_for_every_stream(self):
        for stream_name in STREAMS:
            path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
            with self.subTest(stream=stream_name):
                self.assertTrue(
                    os.path.exists(path),
                    f"Schema file missing for stream '{stream_name}': {path}"
                )

    def test_schema_files_are_valid_json(self):
        for stream_name in STREAMS:
            path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
            if not os.path.exists(path):
                continue
            with self.subTest(stream=stream_name):
                try:
                    with open(path) as fh:
                        schema = json.load(fh)
                except json.JSONDecodeError as exc:
                    self.fail(f"Invalid JSON in '{path}': {exc}")

    def test_schema_root_is_object_type(self):
        for stream_name in STREAMS:
            path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                schema = json.load(fh)
            with self.subTest(stream=stream_name):
                self.assertEqual(
                    schema.get("type"), "object",
                    f"Root type of '{stream_name}' schema must be 'object'"
                )

    def test_schema_has_properties(self):
        for stream_name in STREAMS:
            path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                schema = json.load(fh)
            with self.subTest(stream=stream_name):
                self.assertIn("properties", schema,
                              f"Schema for '{stream_name}' missing 'properties'")
                self.assertGreater(len(schema["properties"]), 0)


# ---------------------------------------------------------------------------
# 7 – Regression: array items must allow null
# ---------------------------------------------------------------------------

class TestSchemaArrayItemsAllowNull(unittest.TestCase):
    """
    Regression test for the prod workspaces crash.

    Every array whose items type includes 'object' must also include 'null'
    so that singer's transform does not raise SchemaMismatch when the Monday
    API returns a null entry inside the array (e.g. a deleted team still
    referenced in teams_subscribers).
    """

    def test_all_array_items_allow_null(self):
        all_schemas = _load_all_schemas()
        for stream_name, schema in all_schemas.items():
            bad_paths = _collect_bad_items(schema)
            with self.subTest(stream=stream_name):
                self.assertEqual(
                    bad_paths, [],
                    f"Stream '{stream_name}' has array items that don't allow null "
                    f"(missing 'null' in type). Affected paths: {bad_paths}. "
                    f"Fix: change \"type\": \"object\" to \"type\": [\"null\", \"object\"] "
                    f"for those items schemas."
                )


# ---------------------------------------------------------------------------
# 8 – get_schemas() structure
# ---------------------------------------------------------------------------

class TestGetSchemas(unittest.TestCase):
    """get_schemas() must return correctly structured dicts for all streams."""

    @classmethod
    def setUpClass(cls):
        cls.schemas, cls.field_metadata = get_schemas()

    def test_returns_all_streams(self):
        self.assertEqual(set(self.schemas.keys()), set(STREAMS.keys()))

    def test_field_metadata_keys_match_streams(self):
        self.assertEqual(set(self.field_metadata.keys()), set(STREAMS.keys()))

    def test_each_schema_is_dict(self):
        for name, schema in self.schemas.items():
            with self.subTest(stream=name):
                self.assertIsInstance(schema, dict)

    def test_each_field_metadata_is_list(self):
        for name, mdata in self.field_metadata.items():
            with self.subTest(stream=name):
                self.assertIsInstance(mdata, list,
                                      f"field_metadata['{name}'] should be a list")

    def test_key_properties_in_metadata(self):
        from singer import metadata as md
        for name, mdata in self.field_metadata.items():
            mdata_map = md.to_map(mdata)
            key_props = mdata_map.get((), {}).get("table-key-properties")
            with self.subTest(stream=name):
                self.assertIsNotNone(
                    key_props,
                    f"'table-key-properties' missing in root metadata for '{name}'"
                )
                self.assertEqual(
                    key_props,
                    list(STREAMS[name].key_properties)
                )


# ---------------------------------------------------------------------------
# 9 – Error propagation
# ---------------------------------------------------------------------------

class TestDiscoverErrorHandling(unittest.TestCase):
    """discover() must propagate errors raised by get_schemas()."""

    @patch("tap_monday.discover.get_schemas", side_effect=FileNotFoundError("missing schema"))
    def test_propagates_file_not_found(self, _mock):
        with self.assertRaises(FileNotFoundError):
            discover()

    @patch("tap_monday.discover.get_schemas", side_effect=json.JSONDecodeError("bad json", "", 0))
    def test_propagates_json_decode_error(self, _mock):
        with self.assertRaises(json.JSONDecodeError):
            discover()

    @patch("tap_monday.discover.get_schemas")
    def test_propagates_schema_key_error(self, mock_get_schemas):
        # Simulate a stream whose schema dict raises KeyError on Schema.from_dict
        mock_get_schemas.return_value = (
            {"bad_stream": None},    # None will cause Schema.from_dict to fail
            {"bad_stream": []}
        )
        with self.assertRaises(Exception):
            discover()


# ---------------------------------------------------------------------------
# 10 – Schema properties ↔ CatalogEntry consistency
# ---------------------------------------------------------------------------

class TestSchemaPropertyConsistency(unittest.TestCase):
    """CatalogEntry schema properties must match the raw schema files."""

    @classmethod
    def setUpClass(cls):
        cls.catalog = discover()
        cls.entries = {e.stream: e for e in cls.catalog.streams}
        cls.raw_schemas = _load_all_schemas()

    def test_catalog_schema_properties_match_raw_file(self):
        for name, entry in self.entries.items():
            raw = self.raw_schemas.get(name, {})
            expected_fields = set(raw.get("properties", {}).keys())
            actual_fields = set(entry.schema.to_dict().get("properties", {}).keys())
            with self.subTest(stream=name):
                self.assertEqual(
                    actual_fields, expected_fields,
                    f"CatalogEntry schema properties for '{name}' differ from raw schema file"
                )
