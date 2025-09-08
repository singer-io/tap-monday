
from base import MondayBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class MondayInterruptedSyncTest(MondayBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_monday_interrupted_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()


    def manipulate_state(self):
        return {
            "currently_syncing": "prospects",
            "bookmarks": {
                "boards": { "updated_at" : "2020-01-01T00:00:00Z"},
                "board_activity_logs": { "created_at" : "2020-01-01T00:00:00Z"},
                "board_items": { "updated_at" : "2020-01-01T00:00:00Z"},
                "platform_api": { "last_updated" : "2020-01-01T00:00:00Z"},
                "reply": { "updated_at" : "2020-01-01T00:00:00Z"},
                "updates": { "updated_at" : "2020-01-01T00:00:00Z"}
            }
        }