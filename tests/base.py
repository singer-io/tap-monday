import copy
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class MondayBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """    
    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-monday"
    
    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.monday"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "account": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "assets": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "audit_event_catalogue": {
                cls.PRIMARY_KEYS: { "name" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "boards": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "updated_at" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "board_activity_logs": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "created_at" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "board_columns": {
                cls.PRIMARY_KEYS: { "id, board_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "board_groups": {
                cls.PRIMARY_KEYS: { "id, board_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "board_items": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "updated_at" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "board_views": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "column_values": {
                cls.PRIMARY_KEYS: { "id, item_id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "docs": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "folders": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "reply": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "updated_at" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "tags": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "teams": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "updates": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "updated_at" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "users": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "workspaces": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {'api_token': 'TAP_MONDAY_API_TOKEN'}

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2022-07-01T00:00:00Z"
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value
