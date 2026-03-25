"""Unit tests for tap_monday.sync — resume/normalization logic.

These tests exercise three behavioural areas of the ``sync()`` function that
were introduced when parent-stream injection, child-stream skipping, and
stale-``currently_syncing`` clearing were added:

1. **Parent injection** – selecting only a child stream causes its parent to be
   added to the sync queue and driven autonomously; the child itself is skipped
   inside the loop because it is driven by the parent.

2. **Stale currently_syncing clearing** – when the ``currently_syncing`` entry
   in the state holds either a child-stream name or a name that does not
   correspond to any root stream, the entry is cleared and the sync runs from
   the beginning so the tap is never permanently wedged.

3. **Resume behaviour** – when ``currently_syncing`` holds a valid root-stream
   name, every root stream that precedes it in the queue is skipped; the
   named stream, plus any subsequent streams, are then synced normally; and
   the ``currently_syncing`` entry is absent from the state when the run ends.
"""

import unittest
from unittest.mock import MagicMock, patch, call

import singer

from tap_monday.sync import sync, update_currently_syncing


# ---------------------------------------------------------------------------
# Helpers shared by all test classes
# ---------------------------------------------------------------------------

def _make_stream_class(name: str, parent: str = "", children=None):
    """Return a minimal stream *class* (not instance) that can be stored in
    STREAMS and instantiated by the real ``sync()`` code.

    Class-level attributes (``parent``, ``children``, ``tap_stream_id``) are
    set so that the pre-processing logic inside ``sync()`` — which inspects the
    class before creating an instance — behaves identically to the real streams.

    The ``sync`` instance method appends *name* to the shared *synced* list
    passed in at creation time, allowing each test to assert exactly which
    streams were driven.
    """
    children = children or []

    def _make(synced_list: list):
        class _FakeStream:
            def __init__(self, client, catalog_entry):
                self.tap_stream_id = name
                self.parent = parent
                self.children = children
                self.child_to_sync = []

            def is_selected(self) -> bool:
                return True

            def write_schema(self) -> None:  # noqa: D401
                """No-op; avoids touching singer I/O in unit tests."""

            def sync(self, state, transformer):  # noqa: D401
                synced_list.append(name)
                return (0, state)

        # Mirror class-level attributes so ``getattr(STREAMS.get(name), "parent")``
        # (and similar introspection inside sync()) returns the right value.
        _FakeStream.parent = parent
        _FakeStream.children = children
        _FakeStream.tap_stream_id = name
        _FakeStream.__name__ = name
        return _FakeStream

    return _make


def _build_fake_streams(*specs, synced_list: list):
    """Build a STREAMS-like dict from a sequence of ``(name, parent, children)``
    3-tuples, wiring every stream class to *synced_list* for call tracking."""
    result = {}
    for spec in specs:
        if len(spec) == 1:
            name, parent, children = spec[0], "", []
        elif len(spec) == 2:
            name, parent, children = spec[0], spec[1], []
        else:
            name, parent, children = spec
        factory = _make_stream_class(name, parent, children)
        result[name] = factory(synced_list)
    return result


def _make_catalog(selected_stream_names):
    """Return a ``singer.Catalog`` mock that reports *selected_stream_names* as
    the selected streams and accepts any ``get_stream`` call."""
    catalog = MagicMock(spec=singer.Catalog)
    entries = [MagicMock(stream=name) for name in selected_stream_names]
    catalog.get_selected_streams.return_value = entries
    catalog.get_stream.side_effect = lambda n: MagicMock()
    return catalog


def _make_client():
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z"}
    return client


# Convenience decorator: suppress all singer I/O (write_state, Transformer)
# so unit tests don't touch stdout or perform real JSON transforms.
def _patch_singer_io(fn):
    """Stack the two patches every test needs."""
    fn = patch("tap_monday.sync.singer.write_state")(fn)
    fn = patch(
        "tap_monday.sync.singer.Transformer",
        return_value=MagicMock(__enter__=lambda s: MagicMock(), __exit__=lambda s, *a: False),
    )(fn)
    return fn


# ---------------------------------------------------------------------------
# 1. Parent injection
# ---------------------------------------------------------------------------

class TestParentInjection(unittest.TestCase):
    """Selecting only a child stream must result in its parent being synced."""

    def setUp(self):
        self.synced = []
        # Two streams: root_b is the parent of child_of_b.
        self.fake_streams = _build_fake_streams(
            ("root_b", "", ["child_of_b"]),
            ("child_of_b", "root_b"),
            synced_list=self.synced,
        )

    @patch("tap_monday.sync.singer.write_state")
    @patch(
        "tap_monday.sync.singer.Transformer",
        return_value=MagicMock(__enter__=lambda s: MagicMock(), __exit__=lambda s, *a: False),
    )
    def test_parent_injected_when_only_child_selected(self, mock_tf, mock_ws):
        """Parent is added to the queue and synced when only its child is selected."""
        catalog = _make_catalog(["child_of_b"])  # only the child is selected
        state = {}

        with patch("tap_monday.sync.STREAMS", self.fake_streams):
            sync(client=_make_client(), config={}, catalog=catalog, state=state)

        self.assertIn(
            "root_b",
            self.synced,
            "root_b (parent) should have been synced even though only its child was selected",
        )

    @patch("tap_monday.sync.singer.write_state")
    @patch(
        "tap_monday.sync.singer.Transformer",
        return_value=MagicMock(__enter__=lambda s: MagicMock(), __exit__=lambda s, *a: False),
    )
    def test_child_not_independently_synced(self, mock_tf, mock_ws):
        """The child stream must NOT appear in the top-level sync() call list
        because it is driven by its parent, not by the outer loop."""
        catalog = _make_catalog(["child_of_b"])
        state = {}

        with patch("tap_monday.sync.STREAMS", self.fake_streams):
            sync(client=_make_client(), config={}, catalog=catalog, state=state)

        self.assertNotIn(
            "child_of_b",
            self.synced,
            "child_of_b should not be independently synced by the outer loop",
        )

    @patch("tap_monday.sync.singer.write_state")
    @patch(
        "tap_monday.sync.singer.Transformer",
        return_value=MagicMock(__enter__=lambda s: MagicMock(), __exit__=lambda s, *a: False),
    )
    def test_parent_injected_only_once(self, mock_tf, mock_ws):
        """When both the parent and child are explicitly selected the parent
        must not be duplicated in the queue (it would be synced twice)."""
        catalog = _make_catalog(["root_b", "child_of_b"])
        state = {}

        with patch("tap_monday.sync.STREAMS", self.fake_streams):
            sync(client=_make_client(), config={}, catalog=catalog, state=state)

        self.assertEqual(
            self.synced.count("root_b"),
            1,
            "root_b should appear exactly once in synced streams",
        )


# ---------------------------------------------------------------------------
# 2. Stale currently_syncing clearing
# ---------------------------------------------------------------------------

class TestStaleSyncingCleared(unittest.TestCase):
    """A currently_syncing value that is not a root stream must be cleared so
    the tap can make progress instead of skipping every stream forever."""

    def setUp(self):
        self.synced = []
        self.fake_streams = _build_fake_streams(
            ("root_a",),
            ("root_b", "", ["child_of_b"]),
            ("child_of_b", "root_b"),
            synced_list=self.synced,
        )

    def _run_sync(self, selected, currently_syncing):
        catalog = _make_catalog(selected)
        state = {"currently_syncing": currently_syncing}

        with patch("tap_monday.sync.STREAMS", self.fake_streams), \
             patch("tap_monday.sync.singer.write_state"), \
             patch(
                 "tap_monday.sync.singer.Transformer",
                 return_value=MagicMock(
                     __enter__=lambda s: MagicMock(),
                     __exit__=lambda s, *a: False,
                 ),
             ):
            sync(client=_make_client(), config={}, catalog=catalog, state=state)

        return state

    def test_child_stream_name_is_cleared(self):
        """currently_syncing set to a child-stream name is cleared and root
        streams are synced from the beginning."""
        state = self._run_sync(
            selected=["root_b", "child_of_b"],
            currently_syncing="child_of_b",  # child, not a root
        )

        self.assertNotIn(
            "currently_syncing",
            state,
            "currently_syncing should be absent after a clean run",
        )
        self.assertIn("root_b", self.synced, "root_b should have been synced")

    def test_unknown_stream_name_is_cleared(self):
        """currently_syncing set to a stream name that does not exist in
        STREAMS is cleared and the tap proceeds normally."""
        state = self._run_sync(
            selected=["root_a"],
            currently_syncing="stream_that_was_removed",
        )

        self.assertNotIn("currently_syncing", state)
        self.assertIn("root_a", self.synced)

    def test_all_root_streams_synced_after_clear(self):
        """After clearing a stale child currently_syncing, all selected root
        streams are synced — none are skipped."""
        state = self._run_sync(
            selected=["root_a", "root_b", "child_of_b"],
            currently_syncing="child_of_b",
        )

        self.assertIn("root_a", self.synced)
        self.assertIn("root_b", self.synced)

    def test_valid_root_stream_not_cleared(self):
        """A currently_syncing value that IS a root stream must NOT be cleared
        — it is a legitimate resume point."""
        catalog = _make_catalog(["root_a", "root_b"])
        state = {"currently_syncing": "root_b"}

        with patch("tap_monday.sync.STREAMS", self.fake_streams), \
             patch("tap_monday.sync.singer.write_state"), \
             patch(
                 "tap_monday.sync.singer.Transformer",
                 return_value=MagicMock(
                     __enter__=lambda s: MagicMock(),
                     __exit__=lambda s, *a: False,
                 ),
             ):
            sync(client=_make_client(), config={}, catalog=catalog, state=state)

        # root_b is a valid resume point so root_a must have been skipped.
        self.assertNotIn(
            "root_a",
            self.synced,
            "root_a should be skipped when resuming from root_b",
        )
        self.assertIn("root_b", self.synced)


# ---------------------------------------------------------------------------
# 3. Resume behaviour
# ---------------------------------------------------------------------------

class TestResumeBehaviour(unittest.TestCase):
    """When currently_syncing names a root stream, earlier root streams are
    skipped and the named stream (plus subsequent ones) are synced normally."""

    def setUp(self):
        self.synced = []
        # Three root streams in the order they will be iterated.
        self.fake_streams = _build_fake_streams(
            ("stream_one",),
            ("stream_two",),
            ("stream_three",),
            synced_list=self.synced,
        )

    def _run_sync(self, selected, currently_syncing):
        catalog = _make_catalog(selected)
        state = {"currently_syncing": currently_syncing} if currently_syncing else {}

        with patch("tap_monday.sync.STREAMS", self.fake_streams), \
             patch("tap_monday.sync.singer.write_state"), \
             patch(
                 "tap_monday.sync.singer.Transformer",
                 return_value=MagicMock(
                     __enter__=lambda s: MagicMock(),
                     __exit__=lambda s, *a: False,
                 ),
             ):
            sync(client=_make_client(), config={}, catalog=catalog, state=state)

        return state

    def test_earlier_streams_skipped(self):
        """Root streams that appear before the resume target are not synced."""
        self._run_sync(
            selected=["stream_one", "stream_two", "stream_three"],
            currently_syncing="stream_two",
        )

        self.assertNotIn(
            "stream_one",
            self.synced,
            "stream_one precedes the resume target and should be skipped",
        )

    def test_resume_stream_is_synced(self):
        """The stream named in currently_syncing is itself synced."""
        self._run_sync(
            selected=["stream_one", "stream_two", "stream_three"],
            currently_syncing="stream_two",
        )

        self.assertIn("stream_two", self.synced)

    def test_later_streams_also_synced(self):
        """Streams after the resume target are synced normally."""
        self._run_sync(
            selected=["stream_one", "stream_two", "stream_three"],
            currently_syncing="stream_two",
        )

        self.assertIn("stream_three", self.synced)

    def test_resume_order(self):
        """Streams are synced in the expected order: resume target first, then
        the remaining streams (earlier ones were skipped)."""
        self._run_sync(
            selected=["stream_one", "stream_two", "stream_three"],
            currently_syncing="stream_two",
        )

        self.assertEqual(
            self.synced,
            ["stream_two", "stream_three"],
            "Only stream_two and stream_three should have been synced, in that order",
        )

    def test_no_currently_syncing_syncs_all(self):
        """When state has no currently_syncing all root streams are synced."""
        self._run_sync(
            selected=["stream_one", "stream_two", "stream_three"],
            currently_syncing=None,
        )

        self.assertEqual(self.synced, ["stream_one", "stream_two", "stream_three"])

    def test_currently_syncing_cleared_after_run(self):
        """After a successful run currently_syncing must not be present in state."""
        state = self._run_sync(
            selected=["stream_one", "stream_two"],
            currently_syncing="stream_one",
        )

        self.assertNotIn(
            "currently_syncing",
            state,
            "currently_syncing should be removed from state after a complete run",
        )

    def test_resume_from_last_stream(self):
        """Resuming from the last stream in the queue syncs only that stream."""
        self._run_sync(
            selected=["stream_one", "stream_two", "stream_three"],
            currently_syncing="stream_three",
        )

        self.assertEqual(self.synced, ["stream_three"])


if __name__ == "__main__":
    unittest.main()
