from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class BoardGroups(FullTableStream):
    tap_stream_id = "board_groups"
    key_properties = ["id", "board_id"]
    replication_method = "FULL_TABLE"
    data_key = "groups"
    path = "boards"

