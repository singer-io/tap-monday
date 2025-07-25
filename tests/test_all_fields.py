from base import MondayBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}


class MondayAllFields(AllFieldsTest, MondayBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_monday_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)