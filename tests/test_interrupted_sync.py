
from base import MondayBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class MondayInterruptedSyncTest(InterruptedSyncTest, MondayBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_monday_interrupted_sync_test"

    def streams_to_test(self):
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
            "board_activity_logs",
            "account"
        }
        return self.expected_stream_names().difference(streams_to_exclude)


    def manipulate_state(self):
        return {
            "currently_syncing": "boards",
            "bookmarks": {
                "boards": { "updated_at" : "2020-01-01T00:00:00Z"},
                "board_items": { "updated_at" : "2020-01-01T00:00:00Z"},
                "reply": { "updated_at" : "2020-01-01T00:00:00Z"},
                "updates": { "updated_at" : "2020-01-01T00:00:00Z"}
            }
        }