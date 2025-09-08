from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import MondayBaseTest

class MondayPaginationTest(PaginationTest, MondayBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_monday_pagination_test"

    def streams_to_test(self):
        # excluded due to insufficient test data
        streams_to_exclude = {"account", "platform_api", "teams", "users"}
        return self.expected_stream_names().difference(streams_to_exclude)
