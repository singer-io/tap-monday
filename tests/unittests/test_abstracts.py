import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from tap_monday.streams.abstracts import IncrementalStream, FullTableStream
from singer import Transformer


class DummyIncrementalStream(IncrementalStream):
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
    tap_stream_id = "full_table"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ("id",)

    def get_records(self, parent_record=None):
        return [{"id": "10"}, {"id": "11"}]


@pytest.mark.parametrize("stream_class, expected_bookmark_key", [
    (DummyIncrementalStream, "updated_at"),
    (DummyFullTableStream, None)
])
@patch("tap_monday.streams.abstracts.write_record")
@patch("tap_monday.streams.abstracts.write_bookmark")
@patch("tap_monday.streams.abstracts.get_bookmark")
@patch("tap_monday.streams.abstracts.metrics.record_counter")
def test_stream_sync(mock_counter, mock_get_bookmark, mock_write_bookmark, mock_write_record,
                     stream_class, expected_bookmark_key):
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

    # Simulate increment tracking
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

