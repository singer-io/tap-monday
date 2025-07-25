from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "data.users"

