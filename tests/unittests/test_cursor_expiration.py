"""Unit tests for Monday.com cursor expiration handling.

Monday.com issues short-lived pagination cursors. When a cursor expires before
the next page is fetched the API returns HTTP 200 with ``errors[].extensions.code
== "CursorException"``.  These tests verify that:

  1. ``raise_for_error`` raises ``MondayCursorExpiredError`` for such responses.
  2. ``BoardItems.sync`` catches the error, resets the cursor, tightens the
     bookmark filter to the latest seen timestamp, and re-drives pagination to
     completion without stopping the tap.
  3. Records already written before the cursor expired are not duplicated on
     the restart (they fall below the updated ``bookmark_date``).
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock, call

from singer import Transformer

from tap_monday.client import raise_for_error
from tap_monday.exceptions import MondayCursorExpiredError
from tap_monday.streams.board_items import BoardItems


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MockResponse:
    """Minimal stand-in for ``requests.Response``."""

    def __init__(self, status_code: int = 200, json_data: dict = None):
        self.status_code = status_code
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


CURSOR_EXPIRED_RESPONSE = MockResponse(
    status_code=200,
    json_data={
        "errors": [
            {
                "message": (
                    "CursorExpiredError: The cursor provided for pagination has "
                    "expired. Please refresh your query and obtain a new cursor to "
                    "continue fetching items"
                ),
                "extensions": {"code": "CursorException"},
            }
        ]
    },
)


def make_board_items_stream(client=None, catalog=None) -> BoardItems:
    """Return a ``BoardItems`` instance wired with minimal mocks."""
    if client is None:
        client = MagicMock()
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        client.base_url = "https://api.monday.com/v2"

    if catalog is None:
        catalog = MagicMock()
        catalog.schema.to_dict.return_value = {
            "properties": {
                "id": {"type": "string"},
                "board_id": {"type": "string"},
                "updated_at": {"type": "string", "format": "date-time"},
                "name": {"type": "string"},
            }
        }
        catalog.metadata = []

    stream = BoardItems(client=client, catalog=catalog)
    stream.metadata = {}
    stream.is_selected = lambda: True
    return stream


# ---------------------------------------------------------------------------
# Test: raise_for_error raises MondayCursorExpiredError
# ---------------------------------------------------------------------------

class TestRaiseForErrorCursorExpired(unittest.TestCase):
    """Verify that raise_for_error detects CursorException responses."""

    def test_cursor_exception_raises_monday_cursor_expired_error(self):
        """A HTTP-200 response with CursorException code raises MondayCursorExpiredError."""
        with self.assertRaises(MondayCursorExpiredError):
            raise_for_error(CURSOR_EXPIRED_RESPONSE)

    def test_cursor_exception_error_message_preserved(self):
        """The raised MondayCursorExpiredError carries the original error message."""
        with self.assertRaises(MondayCursorExpiredError) as ctx:
            raise_for_error(CURSOR_EXPIRED_RESPONSE)
        self.assertIn("CursorException", str(ctx.exception))

    def test_non_cursor_error_does_not_raise_cursor_expired(self):
        """A generic 200-with-errors response should NOT raise MondayCursorExpiredError."""
        generic_error = MockResponse(
            status_code=200,
            json_data={
                "errors": [
                    {
                        "message": "Some other error",
                        "extensions": {"code": "OtherError"},
                    }
                ]
            },
        )
        with self.assertRaises(Exception) as ctx:
            raise_for_error(generic_error)
        self.assertNotIsInstance(ctx.exception, MondayCursorExpiredError)

    def test_clean_200_response_does_not_raise(self):
        """A clean HTTP-200 response with no errors must not raise any exception."""
        ok_response = MockResponse(status_code=200, json_data={"data": {"boards": []}})
        # Should complete without raising
        raise_for_error(ok_response)


# ---------------------------------------------------------------------------
# Test: BoardItems.sync restarts on cursor expiration
# ---------------------------------------------------------------------------

class TestBoardItemsSyncCursorExpiration(unittest.TestCase):
    """Verify BoardItems.sync handles MondayCursorExpiredError gracefully."""

    # ------------------------------------------------------------------
    # Helper: build a stream with mocked get_records
    # ------------------------------------------------------------------

    def _make_stream_with_records(self, batches):
        """
        Return a stream whose ``get_records`` models real cursor-based pagination.

        ``batches`` is a flat list where each element is either:
          - a list of record dicts — a page of records to yield, or
          - ``MondayCursorExpiredError`` — a cursor expiry that interrupts the
            current pagination sequence.

        Each call to ``get_records`` consumes consecutive record-list batches
        and yields their records.  If the *immediately following* batch is a
        ``MondayCursorExpiredError`` sentinel, the error is raised at the end
        of that same generator invocation — matching how the real tap raises
        the error mid-pagination inside a single ``get_records`` call.  A
        subsequent call to ``get_records`` (made by ``sync`` after catching and
        handling the error) then starts from the next batch in the list.
        """
        stream = make_board_items_stream()
        stream.bookmark_value = None

        pos = {"n": 0}

        def fake_get_records(parent_record=None):
            # Yield all consecutive record-list batches for this call.
            while pos["n"] < len(batches) and batches[pos["n"]] is not MondayCursorExpiredError:
                page = batches[pos["n"]]
                pos["n"] += 1
                for r in page:
                    # Inject fields required by BoardItems.object_to_id so that
                    # add_object_to_id does not raise KeyError.
                    yield {"creator": None, "group": None, "parent_item": None, **r}

            # If the next batch is a cursor-expiry sentinel, consume it and
            # raise — simulating expiry while fetching the *next* page.
            if pos["n"] < len(batches) and batches[pos["n"]] is MondayCursorExpiredError:
                pos["n"] += 1
                raise MondayCursorExpiredError("cursor expired")

        stream.get_records = fake_get_records
        return stream

    # ------------------------------------------------------------------
    # Mocking boilerplate
    # ------------------------------------------------------------------

    def _run_sync(self, stream, parent_obj=None, bookmark_date="2024-01-01T00:00:00Z"):
        mock_transformer = MagicMock(spec=Transformer)
        mock_transformer.transform.side_effect = lambda r, s, m: r

        call_count = {"n": 0}

        def increment_side_effect():
            call_count["n"] += 1

        with patch("tap_monday.streams.board_items.write_record") as mock_write_record, \
             patch("tap_monday.streams.board_items.metrics.record_counter") as mock_counter, \
             patch("tap_monday.streams.abstracts.get_bookmark", return_value=bookmark_date), \
             patch("tap_monday.streams.abstracts.write_bookmark", side_effect=lambda s, *a, **kw: s):

            mock_counter_ctx = MagicMock()
            mock_counter.return_value.__enter__.return_value = mock_counter_ctx
            mock_counter_ctx.increment.side_effect = lambda: increment_side_effect()
            type(mock_counter_ctx).value = PropertyMock(side_effect=lambda: call_count["n"])

            state = {}
            count, new_state = stream.sync(
                state=state,
                transformer=mock_transformer,
                parent_obj=parent_obj or {"id": "board_1"},
            )
            return count, new_state, mock_write_record

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_sync_restarts_after_cursor_expiration(self):
        """Sync must not propagate MondayCursorExpiredError; it should restart."""
        page1 = [
            {"id": "1", "board_id": "board_1", "updated_at": "2024-02-01T00:00:00Z", "name": "A"},
        ]
        page2 = [
            {"id": "2", "board_id": "board_1", "updated_at": "2024-03-01T00:00:00Z", "name": "B"},
        ]
        # Sequence: yield page1, then raise cursor expired, then yield page2 on restart
        batches = [page1, MondayCursorExpiredError, page2]
        stream = self._make_stream_with_records(batches)

        # Should not raise
        try:
            count, _, mock_write_record = self._run_sync(stream)
        except MondayCursorExpiredError:
            self.fail("sync() propagated MondayCursorExpiredError instead of handling it")

    def test_sync_writes_all_records_after_restart(self):
        """After a cursor expiration restart all pages are eventually written."""
        page1 = [
            {"id": "1", "board_id": "board_1", "updated_at": "2024-02-01T00:00:00Z", "name": "A"},
        ]
        page2 = [
            {"id": "2", "board_id": "board_1", "updated_at": "2024-03-01T00:00:00Z", "name": "B"},
        ]
        batches = [page1, MondayCursorExpiredError, page2]
        stream = self._make_stream_with_records(batches)
        stream.child_to_sync = []

        _, _, mock_write_record = self._run_sync(stream)

        written_ids = {call_args[0][1]["id"] for call_args in mock_write_record.call_args_list}
        self.assertIn("1", written_ids)
        self.assertIn("2", written_ids)

    def test_sync_reduces_duplicates_on_restart(self):
        """On restart, bookmark_date is tightened to the max seen timestamp,
        so records already written are not re-emitted."""
        # page1 has record with updated_at=2024-02-01
        page1 = [
            {"id": "1", "board_id": "board_1", "updated_at": "2024-02-01T00:00:00Z", "name": "A"},
        ]
        # After cursor expiry, a restart re-fetches page1 AND then page2
        page1_retry = [
            {"id": "1", "board_id": "board_1", "updated_at": "2024-02-01T00:00:00Z", "name": "A"},
            {"id": "2", "board_id": "board_1", "updated_at": "2024-03-01T00:00:00Z", "name": "B"},
        ]
        batches = [page1, MondayCursorExpiredError, page1_retry]
        stream = self._make_stream_with_records(batches)
        stream.child_to_sync = []

        _, _, mock_write_record = self._run_sync(stream, bookmark_date="2024-01-01T00:00:00Z")

        written_ids = [call_args[0][1]["id"] for call_args in mock_write_record.call_args_list]
        # id=1 should NOT be written twice because bookmark was tightened to
        # 2024-02-01 (exclusive) before the restart.
        self.assertEqual(written_ids.count("1"), 1, "Record id=1 should not be duplicated")
        self.assertIn("2", written_ids)

    def test_sync_handles_multiple_cursor_expirations(self):
        """Sync should survive more than one cursor expiration per board."""
        page1 = [
            {"id": "1", "board_id": "board_1", "updated_at": "2024-02-01T00:00:00Z", "name": "A"},
        ]
        page2 = [
            {"id": "2", "board_id": "board_1", "updated_at": "2024-03-01T00:00:00Z", "name": "B"},
        ]
        page3 = [
            {"id": "3", "board_id": "board_1", "updated_at": "2024-04-01T00:00:00Z", "name": "C"},
        ]
        batches = [
            page1,
            MondayCursorExpiredError,
            page2,
            MondayCursorExpiredError,
            page3,
        ]
        stream = self._make_stream_with_records(batches)
        stream.child_to_sync = []

        try:
            count, _, mock_write_record = self._run_sync(stream)
        except MondayCursorExpiredError:
            self.fail("sync() propagated MondayCursorExpiredError on second expiry")

        written_ids = {call_args[0][1]["id"] for call_args in mock_write_record.call_args_list}
        self.assertIn("3", written_ids)

    def test_sync_cursor_reset_on_expiration(self):
        """After cursor expiration the cursor attribute must be reset to None."""
        page1 = [
            {"id": "1", "board_id": "board_1", "updated_at": "2024-02-01T00:00:00Z", "name": "A"},
        ]
        page2 = [
            {"id": "2", "board_id": "board_1", "updated_at": "2024-03-01T00:00:00Z", "name": "B"},
        ]
        cursor_after_expiry = []

        batches = [page1, MondayCursorExpiredError, page2]
        stream = self._make_stream_with_records(batches)
        stream.child_to_sync = []

        original_update_payload = stream.update_data_payload

        def tracking_update_data_payload(*args, **kwargs):
            cursor_after_expiry.append(stream.cursor)
            return original_update_payload(*args, **kwargs)

        stream.update_data_payload = tracking_update_data_payload

        self._run_sync(stream)

        # The first call to update_data_payload after the expiry should see cursor=None
        self.assertIn(None, cursor_after_expiry, "cursor must be None when restarting after expiry")


class TestBoardItemsSyncNoExpiration(unittest.TestCase):
    """Sanity-check that normal (no cursor expiry) sync still works."""

    def test_normal_sync_completes_without_error(self):
        stream = make_board_items_stream()
        stream.bookmark_value = None
        stream.child_to_sync = []

        records = [
            {"id": "10", "board_id": "b1", "updated_at": "2024-06-01T00:00:00Z", "name": "X",
             "creator": None, "group": None, "parent_item": None},
            {"id": "11", "board_id": "b1", "updated_at": "2024-07-01T00:00:00Z", "name": "Y",
             "creator": None, "group": None, "parent_item": None},
        ]

        def fake_get_records(parent_record=None):
            yield from records

        stream.get_records = fake_get_records

        mock_transformer = MagicMock(spec=Transformer)
        mock_transformer.transform.side_effect = lambda r, s, m: r

        written = []

        with patch("tap_monday.streams.board_items.write_record", side_effect=lambda sid, r: written.append(r)), \
             patch("tap_monday.streams.board_items.metrics.record_counter") as mock_counter, \
             patch("tap_monday.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"), \
             patch("tap_monday.streams.abstracts.write_bookmark", side_effect=lambda s, *a, **kw: s):

            mock_ctx = MagicMock()
            mock_counter.return_value.__enter__.return_value = mock_ctx
            type(mock_ctx).value = PropertyMock(return_value=len(records))

            state = {}
            count, _ = stream.sync(state=state, transformer=mock_transformer, parent_obj={"id": "b1"})

        self.assertEqual(len(written), 2)
        self.assertIn("10", {r["id"] for r in written})
        self.assertIn("11", {r["id"] for r in written})


if __name__ == "__main__":
    unittest.main()
