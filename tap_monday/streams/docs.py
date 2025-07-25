from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Docs(FullTableStream):
    tap_stream_id = "docs"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "data.docs"

