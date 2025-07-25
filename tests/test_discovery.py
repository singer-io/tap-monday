"""Test tap discovery mode and metadata."""
from base import MondayBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class MondayDiscoveryTest(DiscoveryTest, MondayBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_monday_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()