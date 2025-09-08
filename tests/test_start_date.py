from base import MondayBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



class MondayStartDateTest(StartDateTest, MondayBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_monday_start_date_test"

    def streams_to_test(self):
        # Skip streams due to lack of test data
        streams_to_exclude = {
            "account",
            "assets",
            "audit_event_catalogue",
            "board_columns",
            "board_groups",
            "board_views",
            "column_values",
            "docs",
            "folders",
            "tags",
            "teams",
            "platform_api",
            "users",
            "workspaces",
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2015-03-25T00:00:00Z"

    @property
    def start_date_2(self):
        return "2023-09-01T00:00:00Z"
