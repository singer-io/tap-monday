import unittest
from unittest.mock import patch, MagicMock, PropertyMock
from parameterized import parameterized

from tap_monday.streams.abstracts import IncrementalStream, FullTableStream
from singer import Transformer
from tap_monday.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MondayError
)


class DummyIncrementalStream(IncrementalStream):
    """A dummy implementation of an Incremental stream for testing."""
    tap_stream_id = "dummy_stream"
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    key_properties = ("id",)

    def get_records(self, parent_record=None):
        return [
            {"id": "1", "updated_at": "2023-01-01T00:00:00Z"},
            {"id": "2", "updated_at": "2024-01-01T00:00:00Z"}
        ]


class DummyFullTableStream(FullTableStream):
    """A dummy implementation of a full-table stream for testing."""
    tap_stream_id = "full_table"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ("id",)

    def get_records(self, parent_record=None):
        return [{"id": "10"}, {"id": "11"}]


class DummyPaginationStream(FullTableStream):
    """A dummy implementation for testing pagination functionality."""
    tap_stream_id = "dummy_stream"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ("id",)


class TestStreamSync(unittest.TestCase):
    """Test suite for validating the stream synchronization process."""

    @parameterized.expand([
        ("Incremental Stream Sync", DummyIncrementalStream, "updated_at"),
        ("FullTable Stream Sync", DummyFullTableStream, None),
    ])
    @patch("tap_monday.streams.abstracts.write_record")
    @patch("tap_monday.streams.abstracts.write_bookmark")
    @patch("tap_monday.streams.abstracts.get_bookmark")
    @patch("tap_monday.streams.abstracts.metrics.record_counter")
    def test_stream_sync(
        self,
        name,
        stream_class,
        expected_bookmark_key,
        mock_counter,
        mock_get_bookmark,
        mock_write_bookmark,
        mock_write_record
    ):
        mock_transformer = MagicMock(spec=Transformer)
        mock_transformer.transform.side_effect = lambda r, s, m: r

        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {
            "properties": {
                "id": {"type": "string"},
                "updated_at": {"type": "string", "format": "date-time"}
            } if expected_bookmark_key else {
                "properties": {"id": {"type": "string"}}
            }
        }

        mock_counter_ctx = MagicMock()
        mock_counter.return_value.__enter__.return_value = mock_counter_ctx

        def increment_side_effect(*args, **kwargs):
            increment_side_effect.call_count += 1

        increment_side_effect.call_count = 0
        mock_counter_ctx.increment.side_effect = increment_side_effect

        type(mock_counter_ctx).value = PropertyMock(
            side_effect=lambda: increment_side_effect.call_count
        )

        stream = stream_class(client=MagicMock(), catalog=mock_catalog)
        stream.metadata = {}
        state = {}

        if expected_bookmark_key:
            mock_get_bookmark.return_value = "2023-01-01T00:00:00Z"
            mock_write_bookmark.return_value = {
                stream_class.tap_stream_id: {
                    expected_bookmark_key: "2024-01-01T00:00:00Z"
                }
            }

        result, new_state = stream.sync(state, mock_transformer)
        self.assertEqual(result, increment_side_effect.call_count)
        self.assertGreater(mock_transformer.transform.call_count, 0)

        if expected_bookmark_key:
            mock_write_bookmark.assert_called_with(
                state,
                stream.tap_stream_id,
                expected_bookmark_key,
                "2024-01-01T00:00:00Z"
            )
            self.assertIn(stream.tap_stream_id, new_state)
        else:
            mock_write_bookmark.assert_not_called()


class TestPagination(unittest.TestCase):
    """Test suite for validating pagination logic in data streams."""

    @parameterized.expand([
        ("Paginated Stream", True, [{"id": "1"}, {"id": "2"}], 2, 2, 2),
        ("Paginated Single Page", True, [{"id": "1"}], 2, None, 1),
        ("Paginated No Data", True, [], 2, None, 1),
        ("No Pagination Stream", False, [{"id": "1"}, {"id": "2"}], 2, None, 1)
    ])
    def test_update_pagination_key(
        self,
        name,
        pagination_supported,
        raw_records,
        page_size,
        expected_next_page,
        expected_request_count
    ):
        mock_client = MagicMock()
        pages = [
            {"data": {"items": raw_records[i:i + page_size]}}
            for i in range(0, len(raw_records), page_size)
        ]
        pages.append({"data": {"items": []}})
        mock_client.make_request.side_effect = pages

        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {
            "properties": {
                "id": {"type": ["null", "string"]}
            }
        }

        stream = DummyPaginationStream(client=mock_client, catalog=mock_catalog)
        stream.get_dot_path_value = MagicMock(
            side_effect=lambda response, key: response["data"]["items"]
        )
        stream.parse_raw_records = MagicMock(side_effect=lambda records: records)
        stream.update_data_payload = MagicMock()

        stream.pagination_supported = pagination_supported
        stream.page_size = page_size
        stream._graphql_query = "dummy_query"

        records = list(stream.get_records(parent_record={}))
        self.assertEqual(records, raw_records)
        self.assertEqual(mock_client.make_request.call_count, expected_request_count)

        if expected_next_page is not None:
            stream.update_data_payload.assert_called_with("dummy_query", {}, page=expected_next_page)
        else:
            stream.update_data_payload.assert_not_called()


class TestGraphQLQueryGeneration(unittest.TestCase):
    """Test suite for validating GraphQL query generation from JSON schemas."""

    @parameterized.expand([
        (
            "Simple flat schema",
            {"properties": {"id": {"type": "string"}, "name": {"type": "string"}}},
            [],
            {},
            "items",
            "query { items { id name }}"
        ),
        (
            "Nested with extras",
            {
                "properties": {
                    "id": {"type": "string"},
                    "details": {
                        "type": "object",
                        "properties": {
                            "field1": {"type": "string"},
                            "field2": {"type": "string"},
                        }
                    }
                }
            },
            [],
            {"details.extra1": [], "details.extra2": []},
            "items",
            "query { items { details { extra1 extra2 field1 field2 } id }}"
        ),
        (
            "Excluded field",
            {
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "extra": {"type": "string"}
                }
            },
            ["extra"],
            {},
            "items",
            "query { items { id name }}"
        ),
    ])
    def test_get_graphql_query(
        self,
        name,
        schema,
        excluded_fields,
        extra_fields,
        root_field,
        expected_query
    ):
        mock_client = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = schema

        stream = DummyIncrementalStream(client=mock_client, catalog=mock_catalog)
        stream.excluded_fields = excluded_fields
        stream.extra_fields = extra_fields

        query = stream.get_graphql_query(root_field=root_field, indent=1, level=1)
        self.assertEqual(query, expected_query)


class TestGraphQLMethods(unittest.TestCase):
    """
    Test suite for internal GraphQL utility methods used in stream classes.

    Includes tests for:
    - Detecting object and array-of-object types
    - Building extra field trees from dot-notated keys
    - Filtering out excluded fields
    - Generating valid GraphQL queries based on schema and extra fields
    """

    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_catalog = MagicMock()
        self.obj = DummyIncrementalStream(client=self.mock_client, catalog=self.mock_catalog)

    def test_is_object(self):
        self.assertTrue(self.obj._is_object({"type": "object"}))
        self.assertTrue(self.obj._is_object({"type": ["object"]}))
        self.assertFalse(self.obj._is_object({"type": "string"}))

    def test_is_array_of_objects(self):
        self.assertTrue(self.obj._is_array_of_objects({
            "type": "array",
            "items": {"type": "object"}
        }))
        self.assertFalse(self.obj._is_array_of_objects({
            "type": "array",
            "items": {"type": "string"}
        }))
        self.assertFalse(self.obj._is_array_of_objects({
            "type": "string"
        }))

    def test_collect_extra_tree(self):
        extra_fields = {
            "a.b": ["x"],
            "a.c": ["y", "z"],
            "d": ["w"]
        }
        expected_tree = {
            "a": {
                "b": {"_extras": ["x"]},
                "c": {"_extras": ["y", "z"]}
            },
            "d": {"_extras": ["w"]}
        }
        self.assertEqual(self.obj._collect_extra_tree(extra_fields), expected_tree)

    def test_process_properties_with_extras(self):
        self.obj.excluded_fields = []
        schema = {
            "properties": {
                "id": {"type": ["null", "string"]},
                "profile": {
                    "type": "object",
                    "properties": {
                        "name": {"type": ["null", "string"]}
                    }
                }
            }
        }
        extras = {
            "profile": {
                "_extras": ["extra_field"]
            }
        }
        result = self.obj._process_properties(schema, depth=1, extras_branch=extras)
        self.assertIn("profile {", result)
        self.assertIn("extra_field", result)

    def test_process_properties_excluded_fields(self):
        self.obj.excluded_fields = ["id"]
        schema = {
            "id": {"type": ["null", "string"]},
            "name": {"type": ["null", "string"]}
        }
        result = self.obj._process_properties(schema, depth=1, extras_branch={})
        self.assertNotIn("id", result)
        self.assertIn("name", result)

    def test_get_graphql_query_flat(self):
        self.obj.schema = {
            "properties": {
                "id": {"type": ["null", "string"]},
                "name": {"type": ["null", "string"]},
            }
        }
        self.obj.extra_fields = {}
        self.obj.excluded_fields = []
        result = self.obj.get_graphql_query(root_field="users")
        self.assertEqual(result, "query { users { id name }}")

    def test_get_graphql_query_with_nested_extra(self):
        self.obj.schema = {
            "properties": {
                "profile": {
                    "type": "object",
                    "properties": {
                        "bio": {"type": ["null", "string"]}
                    }
                }
            }
        }
        self.obj.extra_fields = {"profile.address": ["address1", "address2"]}
        self.obj.excluded_fields = []
        result = self.obj.get_graphql_query(root_field="users")
        self.assertIn("profile {", result)
        self.assertIn("address {", result)
        self.assertIn("address2", result)

