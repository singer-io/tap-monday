from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class ColumnValues(FullTableStream):
    tap_stream_id = "column_values"
    key_properties = ["id", "item_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "column_values"
    path = "board_items"

