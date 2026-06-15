from base import MondayBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class MondayBookMarkTest(BookmarkTest, MondayBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "boards": { "updated_at" : "2020-01-01T00:00:00Z"},
            "board_items": { "updated_at" : "2020-01-01T00:00:00Z"},
            "updates": { "updated_at" : "2020-01-01T00:00:00Z"},
            "reply": { "updated_at" : "2020-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_monday_bookmark_test"

    def streams_to_test(self):
        # excluded streams: full_table streams and streams with insufficient test data
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
            "board_activity_logs"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """
        Calculates new bookmarks by looking through sync 1 data to determine a bookmark
        that will sync 2 records in sync 2 (plus any necessary look back data)
        """
        new_bookmarks = super().calculate_new_bookmarks()
        new_bookmarks = {key: {k: v.replace(".000000Z", ".000Z") for k, v in value.items()}
                for key, value in new_bookmarks.items()}

        # Parent streams (ParentChildBookmarkMixin) use min(parent, children) as
        # the effective bookmark. Adjust parent bookmarks to reflect this so the
        # test assertion for records respecting the bookmark uses the correct value.
        parent_children = {
            "boards": ["board_items", "board_activity_logs"],
            "updates": ["reply"],
        }
        for parent, children in parent_children.items():
            if parent not in new_bookmarks:
                continue
            parent_key = next(iter(new_bookmarks[parent]))
            parent_val = new_bookmarks[parent][parent_key]
            for child in children:
                if child in new_bookmarks:
                    child_key = next(iter(new_bookmarks[child]))
                    child_val = new_bookmarks[child][child_key]
                    parent_val = min(parent_val, child_val)
            new_bookmarks[parent][parent_key] = parent_val

        return new_bookmarks
