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
  11. Access checks: inaccessible streams excluded from catalog.
  12. Access checks: child streams removed when parent is excluded.
  13. Access checks: MondayForbiddenError raised when all parent streams blocked.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_monday.discover import discover, _apply_access_checks, _prune_inaccessible_children
from tap_monday.exceptions import MondayForbiddenError, MondayInternalServerError
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

    def setUp(self):
        self.client = _make_mock_client()
        # Make all streams accessible so discover() completes without error
        patcher = patch("tap_monday.discover._apply_access_checks")
        self.mock_access = patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_catalog_instance(self):
        catalog = discover(self.client)
        self.assertIsInstance(catalog, Catalog)

    def test_catalog_has_all_streams(self):
        catalog = discover(self.client)
        catalog_stream_names = {entry.stream for entry in catalog.streams}
        self.assertEqual(catalog_stream_names, set(STREAMS.keys()))

    def test_no_duplicate_streams(self):
        catalog = discover(self.client)
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
        client = _make_mock_client()
        with patch("tap_monday.discover._apply_access_checks"):
            cls.catalog = discover(client)
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
        client = _make_mock_client()
        with patch("tap_monday.discover._apply_access_checks"):
            cls.catalog = discover(client)

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
            client = _make_mock_client()
            with patch("tap_monday.discover._apply_access_checks"):
                catalog = discover(client)
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
                        json.load(fh)
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
            discover(_make_mock_client())

    @patch("tap_monday.discover.get_schemas", side_effect=json.JSONDecodeError("bad json", "", 0))
    def test_propagates_json_decode_error(self, _mock):
        with self.assertRaises(json.JSONDecodeError):
            discover(_make_mock_client())

    @patch("tap_monday.discover.get_schemas")
    def test_propagates_schema_key_error(self, mock_get_schemas):
        # Simulate a stream whose schema dict raises KeyError on Schema.from_dict
        mock_get_schemas.return_value = (
            {"bad_stream": None},    # None will cause Schema.from_dict to fail
            {"bad_stream": []}
        )
        with self.assertRaises(Exception):
            discover(_make_mock_client())


# ---------------------------------------------------------------------------
# 10 – Schema properties ↔ CatalogEntry consistency
# ---------------------------------------------------------------------------

class TestSchemaPropertyConsistency(unittest.TestCase):
    """CatalogEntry schema properties must match the raw schema files."""

    @classmethod
    def setUpClass(cls):
        client = _make_mock_client()
        with patch("tap_monday.discover._apply_access_checks"):
            cls.catalog = discover(client)
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


# ---------------------------------------------------------------------------
# 11 – Access checks: inaccessible streams excluded from catalog
# ---------------------------------------------------------------------------

def _make_mock_client():
    """Return a MagicMock that passes as a tap_monday Client."""
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z"}
    return client


class TestApplyAccessChecks(unittest.TestCase):
    """_apply_access_checks() must mutate schemas/field_metadata in place."""

    def _schemas_and_metadata(self):
        return get_schemas()

    def test_all_accessible_leaves_catalog_intact(self):
        schemas, field_metadata = self._schemas_and_metadata()
        original_keys = set(schemas.keys())
        client = _make_mock_client()
        with patch("tap_monday.discover.STREAMS", {
            name: _make_accessible_stream_cls(name, cls)
            for name, cls in STREAMS.items()
        }):
            _apply_access_checks(client, schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_inaccessible_parent_excluded(self):
        schemas, field_metadata = self._schemas_and_metadata()
        client = _make_mock_client()
        # Make 'audit_event_catalogue' forbidden, all others accessible
        patched = {
            name: _make_forbidden_stream_cls(name, cls) if name == "audit_event_catalogue"
            else _make_accessible_stream_cls(name, cls)
            for name, cls in STREAMS.items()
        }
        with patch("tap_monday.discover.STREAMS", patched):
            _apply_access_checks(client, schemas, field_metadata)
        self.assertNotIn("audit_event_catalogue", schemas)
        self.assertNotIn("audit_event_catalogue", field_metadata)

    def test_inaccessible_stream_warning_is_logged(self):
        schemas, field_metadata = self._schemas_and_metadata()
        client = _make_mock_client()
        patched = {
            name: _make_forbidden_stream_cls(name, cls) if name == "audit_event_catalogue"
            else _make_accessible_stream_cls(name, cls)
            for name, cls in STREAMS.items()
        }
        with patch("tap_monday.discover.STREAMS", patched):
            with patch("tap_monday.discover.LOGGER") as mock_logger:
                _apply_access_checks(client, schemas, field_metadata)
                mock_logger.warning.assert_called()
                warning_call_args = " ".join(
                    str(a) for a in mock_logger.warning.call_args_list[-1][0]
                )
                self.assertIn("audit_event_catalogue", warning_call_args)

    def test_all_parents_inaccessible_raises_forbidden_error(self):
        schemas, field_metadata = self._schemas_and_metadata()
        client = _make_mock_client()
        # Make every parent stream forbidden
        patched = {
            name: _make_forbidden_stream_cls(name, cls) if not cls.parent
            else _make_accessible_stream_cls(name, cls)
            for name, cls in STREAMS.items()
        }
        with patch("tap_monday.discover.STREAMS", patched):
            with self.assertRaises(MondayForbiddenError):
                _apply_access_checks(client, schemas, field_metadata)

    def test_all_parents_inaccessible_via_500_raises_internal_server_error(self):
        """When all failures are 500s (not 403s), MondayInternalServerError must be raised."""
        schemas, field_metadata = self._schemas_and_metadata()
        client = _make_mock_client()

        def _make_500_stream_cls(name, original_cls):
            class _ServerError(original_cls):
                def check_access(self):
                    return MondayInternalServerError("500")
            _ServerError.parent = original_cls.parent
            _ServerError.children = original_cls.children
            _ServerError.tap_stream_id = original_cls.tap_stream_id
            _ServerError.__name__ = original_cls.__name__
            return _ServerError

        patched = {
            name: _make_500_stream_cls(name, cls) if not cls.parent
            else _make_accessible_stream_cls(name, cls)
            for name, cls in STREAMS.items()
        }
        with patch("tap_monday.discover.STREAMS", patched):
            with self.assertRaises(MondayInternalServerError):
                _apply_access_checks(client, schemas, field_metadata)

    def test_discover_with_client_runs_access_checks(self):
        """discover(client) must call _apply_access_checks."""
        client = _make_mock_client()
        with patch("tap_monday.discover._apply_access_checks") as mock_check:
            discover(client)
            mock_check.assert_called_once()
            args = mock_check.call_args[0]
            self.assertIs(args[0], client)

    def test_discover_without_client_skips_access_checks(self):
        """discover() always calls _apply_access_checks — client is required."""
        with self.assertRaises(TypeError):
            discover()


# ---------------------------------------------------------------------------
# 12 – Child streams removed when parent is excluded
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):
    """_prune_inaccessible_children() must remove orphaned child streams."""

    def _schemas_and_metadata(self):
        return get_schemas()

    def test_children_of_excluded_parent_are_removed(self):
        schemas, field_metadata = self._schemas_and_metadata()
        # Simulate 'boards' being excluded — its children should be pruned
        schemas.pop("boards", None)
        field_metadata.pop("boards", None)
        _prune_inaccessible_children(schemas, field_metadata)
        board_children = [
            name for name, cls in STREAMS.items()
            if cls.parent == "boards"
        ]
        for child in board_children:
            with self.subTest(child=child):
                self.assertNotIn(child, schemas)
                self.assertNotIn(child, field_metadata)

    def test_grandchildren_excluded_when_parent_excluded(self):
        """column_values is a child of board_items which is a child of boards."""
        schemas, field_metadata = self._schemas_and_metadata()
        # Remove boards — its child board_items gets pruned first
        schemas.pop("boards", None)
        field_metadata.pop("boards", None)
        _prune_inaccessible_children(schemas, field_metadata)
        # board_items is now gone; column_values (child of board_items) must also go
        # Re-apply to simulate cascading
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertNotIn("board_items", schemas)
        self.assertNotIn("column_values", schemas)

    def test_accessible_streams_unaffected_by_pruning(self):
        schemas, field_metadata = self._schemas_and_metadata()
        before = set(schemas.keys())
        _prune_inaccessible_children(schemas, field_metadata)
        # No parent was removed, so nothing should be pruned
        self.assertEqual(set(schemas.keys()), before)

    def test_child_warning_logged_when_pruned(self):
        schemas, field_metadata = self._schemas_and_metadata()
        schemas.pop("boards", None)
        field_metadata.pop("boards", None)
        with patch("tap_monday.discover.LOGGER") as mock_logger:
            _prune_inaccessible_children(schemas, field_metadata)
            mock_logger.warning.assert_called()


# ---------------------------------------------------------------------------
# 13 – BaseStream.check_access()
# ---------------------------------------------------------------------------

class TestBaseStreamCheckAccess(unittest.TestCase):
    """BaseStream.check_access() must return None or an exception based on API response."""

    def _make_stream_instance(self, stream_name, forbidden=False):
        """Instantiate a stream with a mock client that either succeeds or raises 403."""
        client = _make_mock_client()
        if forbidden:
            client.probe_request.side_effect = MondayForbiddenError("403 Forbidden")
        stream_cls = STREAMS[stream_name]
        return stream_cls(client=client)

    def test_accessible_stream_returns_none(self):
        stream = self._make_stream_instance("account", forbidden=False)
        self.assertIsNone(stream.check_access())

    def test_forbidden_stream_returns_exception(self):
        stream = self._make_stream_instance("account", forbidden=True)
        self.assertIsInstance(stream.check_access(), MondayForbiddenError)

    def test_internal_server_error_stream_returns_exception(self):
        stream = self._make_stream_instance("account")
        stream.client.probe_request.side_effect = MondayInternalServerError("500")
        self.assertIsInstance(stream.check_access(), MondayInternalServerError)

    def test_child_stream_always_returns_none(self):
        """Child streams skip the HTTP probe and always return None."""
        # board_columns has parent="boards"
        child_stream = self._make_stream_instance("board_columns", forbidden=True)
        self.assertIsNone(child_stream.check_access())
        # Even though probe_request would raise 403, it should never be called
        child_stream.client.probe_request.assert_not_called()

    def test_check_access_makes_request_for_parent(self):
        stream = self._make_stream_instance("audit_event_catalogue", forbidden=False)
        stream.check_access()
        stream.client.probe_request.assert_called_once()

    def test_check_access_logs_warning_on_forbidden(self):
        stream = self._make_stream_instance("account", forbidden=True)
        with patch("tap_monday.streams.abstracts.LOGGER") as mock_logger:
            result = stream.check_access()
            self.assertIsInstance(result, MondayForbiddenError)
            mock_logger.warning.assert_called_once()


# ---------------------------------------------------------------------------
# Helper factories used by access-check tests
# ---------------------------------------------------------------------------

def _make_accessible_stream_cls(name, original_cls):
    """Return a subclass of the original that overrides check_access to return None (accessible)."""
    class _Accessible(original_cls):
        def check_access(self):
            return None
    _Accessible.parent = original_cls.parent
    _Accessible.children = original_cls.children
    _Accessible.tap_stream_id = original_cls.tap_stream_id
    _Accessible.__name__ = original_cls.__name__
    return _Accessible


def _make_forbidden_stream_cls(name, original_cls):
    """Return a subclass of the original that overrides check_access to return a MondayForbiddenError."""
    class _Forbidden(original_cls):
        def check_access(self):
            return MondayForbiddenError("403 Forbidden")
    _Forbidden.parent = original_cls.parent
    _Forbidden.children = original_cls.children
    _Forbidden.tap_stream_id = original_cls.tap_stream_id
    _Forbidden.__name__ = original_cls.__name__
    return _Forbidden
