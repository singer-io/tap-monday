from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Tags(FullTableStream):
    tap_stream_id = "tags"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.tags"

