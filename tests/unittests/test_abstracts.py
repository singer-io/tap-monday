import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from tap_monday.streams.abstracts import IncrementalStream, FullTableStream
from singer import Transformer


class DummyIncrementalStream(IncrementalStream):
    """A dummy implementation of a Incremental stream for testing."""
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
    """A dummy implementation of a Incremental stream for testing."""
    tap_stream_id = "dummy_stream"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ("id",)


@pytest.mark.parametrize("stream_class, expected_bookmark_key", [
    pytest.param(DummyIncrementalStream, "updated_at", id="Incremental Stream Sync"),
    pytest.param(DummyFullTableStream, None, id="FullTable Stream Sync")
])
@patch("tap_monday.streams.abstracts.write_record")
@patch("tap_monday.streams.abstracts.write_bookmark")
@patch("tap_monday.streams.abstracts.get_bookmark")
@patch("tap_monday.streams.abstracts.metrics.record_counter")
def test_stream_sync(mock_counter, mock_get_bookmark, mock_write_bookmark, mock_write_record,
                     stream_class, expected_bookmark_key):
    """
    Test the sync method of both incremental and full-table stream classes.
    """
    mock_transformer = MagicMock(spec=Transformer)
    mock_transformer.transform.side_effect = lambda r, s, m: r

    mock_catalog = MagicMock()
    mock_catalog.schema.to_dict.return_value = {
        "properties": {
            "id": {"type": "string"},
            "updated_at": {"type": "string", "format": "date-time"}
            }
            if expected_bookmark_key else {
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
    assert result == increment_side_effect.call_count
    assert mock_transformer.transform.call_count > 0

    if expected_bookmark_key:
        mock_write_bookmark.assert_called_with(
            state,
            stream.tap_stream_id,
            expected_bookmark_key,
            "2024-01-01T00:00:00Z"
        )
        assert stream.tap_stream_id in new_state
    else:
        mock_write_record.assert_not_called()

@pytest.mark.parametrize(
    "pagination_supported, raw_records, page_size, expected_next_page, expected_request_count",
    [
        pytest.param(True, [{"id": "1"}, {"id": "2"}], 2, 2, 2, id="Paginated Stream"),
        pytest.param(True, [{"id": "1"}], 2, None, 1, id="Paginated Single Page"),
        pytest.param(True, [], 2, None, 1, id="Paginated No Data"),
        pytest.param(False, [{"id": "1"}, {"id": "2"}], 2, None, 1, id="No Pagination Stream")
    ]
)
def test_update_pagination_key(
    pagination_supported,
    raw_records,
    page_size,
    expected_next_page,
    expected_request_count
    ):
    """
    Test that update_pagination_key behaves correctly based on
    pagination flags and data volume.
    """
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
                "properties": {"id": {"type": "string"}}
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
    assert records == raw_records
    assert mock_client.make_request.call_count == expected_request_count
    if expected_next_page is not None:
        stream.update_data_payload.assert_called_with("dummy_query", {}, page=expected_next_page)
    else:
        stream.update_data_payload.assert_not_called()


@pytest.mark.parametrize(
    "schema, excluded_fields, extra_fields, root_field, expected_query",
    [
        pytest.param(
            {
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                }
            },
            [],
            {},
            "items",
            "query { items { id name }}",
            id="Simple flat schema"
        ),
        pytest.param(
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
            "query { items { details { extra1 extra2 field1 field2 } id }}",
            id="Nested with extras"
        ),
        pytest.param(
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
            "query { items { id name }}",
            id="Excluded field"
        ),
    ]
)
def test_get_graphql_query(
    schema,
    excluded_fields,
    extra_fields,
    root_field,
    expected_query
    ):
    """
    Test the get_graphql_query method to ensure it correctly
    generates GraphQL queries from a given JSON schema,
    optionally including additional (non-schema) fields.
    """
    mock_client = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.schema.to_dict.return_value = schema

    stream = DummyIncrementalStream(client=mock_client, catalog=mock_catalog)
    stream.excluded_fields = excluded_fields
    stream.extra_fields = extra_fields

    query = stream.get_graphql_query(root_field=root_field, indent=1, level=1)
    assert query == expected_query

