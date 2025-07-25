from base import MondayBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class MondayBookMarkTest(BookmarkTest, MondayBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "boards": { "updated_at" : "2020-01-01T00:00:00Z"},
            "board_activity_logs": { "created_at" : "2020-01-01T00:00:00Z"},
            "board_items": { "updated_at" : "2020-01-01T00:00:00Z"},
            "reply": { "updated_at" : "2020-01-01T00:00:00Z"},
            "updates": { "updated_at" : "2020-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_monday_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)
