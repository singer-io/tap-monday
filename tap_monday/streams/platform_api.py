from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class PlatformApi(IncrementalStream):
    tap_stream_id = "platform_api"
    key_properties = ["daily_analytics.last_update"]
    replication_method = "INCREMENTAL"
    replication_keys = ["daily_analytics.last_update"]
    data_key = "data.platform_api"
