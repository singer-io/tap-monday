from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class BoardViews(FullTableStream):
    tap_stream_id = "board_views"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "data.boards.views"
    path = "boards"

