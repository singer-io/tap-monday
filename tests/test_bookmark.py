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
        }
    }
    @staticmethod
    def name():
        return "tap_tester_monday_bookmark_test"

    def streams_to_test(self):
        # excluded streams: full_table streams + streams with insufficient test data
        streams_to_exclude = {
            "audit_event_catalogue",
            "column_values",
            "teams",
            "workspaces",
            "tags",
            "folders",
            "board_views",
            "board_groups",
            "board_columns",
            "docs",
            "account",
            "assets",
            "users",
            "platform_api",
            # board_items is a child of boards; the parent bookmark filter can
            # exclude boards whose items would otherwise match, causing zero
            # records in sync 2 and bookmark regression.
            "board_items",
            # updates and reply have too few records with unique replication-key
            # values for calculate_new_bookmarks to produce a meaningful
            # manipulated bookmark (all records still pass the filter).
            "updates",
            "reply",
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """
        Calculates new bookmarks by looking through sync 1 data to determine a bookmark
        that will sync 2 records in sync 2 (plus any necessary look back data)
        """
        new_bookmarks = super().calculate_new_bookmarks()
        return {key: {k: v.replace(".000000Z", ".000Z") for k, v in value.items()}
                for key, value in new_bookmarks.items()}
