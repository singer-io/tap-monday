import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

import singer
from tap_monday.sync import sync
from tap_monday.discover import discover

SCHEMAS_DIR = Path(__file__).parent.parent.parent / "tap_monday" / "schemas"


class TestCurrentlySyncingResume(unittest.TestCase):
    """Test suite for validating currently_syncing resume logic in sync."""

    def _make_catalog(self, stream_names):
        """Create a mock catalog with selected streams."""
        catalog = MagicMock(spec=singer.Catalog)
        entries = []
        for name in stream_names:
            entry = MagicMock()
            entry.stream = name
            entries.append(entry)
        catalog.get_selected_streams.return_value = entries

        def get_stream(name):
            entry = MagicMock()
            entry.schema.to_dict.return_value = {"properties": {"id": {"type": "string"}}}
            entry.metadata = []
            return entry

        catalog.get_stream.side_effect = get_stream
        return catalog

    @patch("tap_monday.sync.write_schema")
    @patch("tap_monday.sync.update_currently_syncing")
    @patch("tap_monday.sync.STREAMS")
    @patch("singer.get_currently_syncing")
    def test_sync_resumes_from_currently_syncing(
        self, mock_get_currently_syncing, mock_streams, mock_update_cs, mock_write_schema
    ):
        """When currently_syncing is set, streams before it should be skipped."""
        mock_get_currently_syncing.return_value = "stream_b"

        # Create mock streams
        def make_stream(name):
            s = MagicMock()
            s.parent = ""
            s.tap_stream_id = name
            s.sync.return_value = (0, {})
            s.is_selected.return_value = True
            return s

        stream_a = make_stream("stream_a")
        stream_b = make_stream("stream_b")

        mock_streams.__getitem__.side_effect = lambda name: {
            "stream_a": lambda client, cat: stream_a,
            "stream_b": lambda client, cat: stream_b,
        }[name]

        catalog = self._make_catalog(["stream_a", "stream_b"])
        state = {}
        client = MagicMock()
        config = {}

        with patch("singer.Transformer") as mock_transformer_cls:
            mock_transformer = MagicMock()
            mock_transformer_cls.return_value.__enter__.return_value = mock_transformer
            sync(client=client, config=config, catalog=catalog, state=state)

        # stream_a should be skipped, stream_b should be synced
        stream_a.sync.assert_not_called()
        stream_b.sync.assert_called_once()

    @patch("tap_monday.sync.write_schema")
    @patch("tap_monday.sync.update_currently_syncing")
    @patch("tap_monday.sync.STREAMS")
    @patch("singer.get_currently_syncing")
    def test_sync_no_skip_when_no_currently_syncing(
        self, mock_get_currently_syncing, mock_streams, mock_update_cs, mock_write_schema
    ):
        """When currently_syncing is None, all streams should be synced."""
        mock_get_currently_syncing.return_value = None

        def make_stream(name):
            s = MagicMock()
            s.parent = ""
            s.tap_stream_id = name
            s.sync.return_value = (0, {})
            s.is_selected.return_value = True
            return s

        stream_a = make_stream("stream_a")
        stream_b = make_stream("stream_b")

        mock_streams.__getitem__.side_effect = lambda name: {
            "stream_a": lambda client, cat: stream_a,
            "stream_b": lambda client, cat: stream_b,
        }[name]

        catalog = self._make_catalog(["stream_a", "stream_b"])
        state = {}
        client = MagicMock()
        config = {}

        with patch("singer.Transformer") as mock_transformer_cls:
            mock_transformer = MagicMock()
            mock_transformer_cls.return_value.__enter__.return_value = mock_transformer
            sync(client=client, config=config, catalog=catalog, state=state)

        # Both streams should be synced
        stream_a.sync.assert_called_once()
        stream_b.sync.assert_called_once()

    @patch("tap_monday.sync.write_schema")
    @patch("tap_monday.sync.update_currently_syncing")
    @patch("tap_monday.sync.STREAMS")
    @patch("singer.get_currently_syncing")
    def test_sync_resumes_from_first_stream(
        self, mock_get_currently_syncing, mock_streams, mock_update_cs, mock_write_schema
    ):
        """When currently_syncing matches the first stream, no skipping happens."""
        mock_get_currently_syncing.return_value = "stream_a"

        def make_stream(name):
            s = MagicMock()
            s.parent = ""
            s.tap_stream_id = name
            s.sync.return_value = (0, {})
            s.is_selected.return_value = True
            return s

        stream_a = make_stream("stream_a")
        stream_b = make_stream("stream_b")

        mock_streams.__getitem__.side_effect = lambda name: {
            "stream_a": lambda client, cat: stream_a,
            "stream_b": lambda client, cat: stream_b,
        }[name]

        catalog = self._make_catalog(["stream_a", "stream_b"])
        state = {}
        client = MagicMock()
        config = {}

        with patch("singer.Transformer") as mock_transformer_cls:
            mock_transformer = MagicMock()
            mock_transformer_cls.return_value.__enter__.return_value = mock_transformer
            sync(client=client, config=config, catalog=catalog, state=state)

        # Both streams should be synced (stream_a is the resume point, so no skipping)
        stream_a.sync.assert_called_once()
        stream_b.sync.assert_called_once()


class TestDiscoverReplicationKey(unittest.TestCase):
    """Test suite for validating the discover logic sets replication_key."""

    @patch("tap_monday.discover.get_schemas")
    def test_discover_sets_replication_key_for_incremental_streams(self, mock_get_schemas):
        """Incremental streams should have replication_key set in catalog entry."""
        mock_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "updated_at": {"type": ["null", "string"], "format": "date-time"}
            }
        }
        # Simulate what get_standard_metadata returns for an incremental stream
        from singer import metadata as md
        mdata = md.new()
        mdata = md.get_standard_metadata(
            schema=mock_schema,
            key_properties=["id"],
            valid_replication_keys=["updated_at"],
            replication_method="INCREMENTAL",
        )
        mdata_map = md.to_map(mdata)
        mdata_map = md.write(mdata_map, ("properties", "updated_at"), "inclusion", "automatic")
        mdata_list = md.to_list(mdata_map)

        mock_get_schemas.return_value = (
            {"boards": mock_schema},
            {"boards": mdata_list}
        )

        catalog = discover()
        self.assertEqual(len(catalog.streams), 1)
        entry = catalog.streams[0]
        self.assertEqual(entry.tap_stream_id, "boards")
        self.assertEqual(entry.replication_key, "updated_at")

    @patch("tap_monday.discover.get_schemas")
    def test_discover_no_replication_key_for_full_table_streams(self, mock_get_schemas):
        """Full-table streams should have replication_key as None in catalog entry."""
        mock_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]}
            }
        }
        from singer import metadata as md
        mdata = md.new()
        mdata = md.get_standard_metadata(
            schema=mock_schema,
            key_properties=["id"],
            valid_replication_keys=[],
            replication_method="FULL_TABLE",
        )
        mdata_list = md.to_list(md.to_map(mdata))

        mock_get_schemas.return_value = (
            {"teams": mock_schema},
            {"teams": mdata_list}
        )

        catalog = discover()
        entry = catalog.streams[0]
        self.assertIsNone(entry.replication_key)
