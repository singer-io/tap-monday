from typing import Dict
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class PlatformApi(IncrementalStream):
    tap_stream_id = "platform_api"
    key_properties = ["last_updated"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_updated"]
    data_key = "data.platform_api"
    root_field = "platform_api"
    excluded_fields = ["last_updated"]
    extra_fields = {
        "daily_analytics.last_updated": []
    }

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["last_updated"] = record["daily_analytics"]["last_updated"]
        return record

