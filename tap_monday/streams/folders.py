from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Folders(FullTableStream):
    tap_stream_id = "folders"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.folders"
    root_field = "folders"
    object_to_id = {"parent": "parent", "workspace": "workspace"}
    extra_fields = {
        "parent": ["id"],
        "workspace": ["id"]
    }
    excluded_fields = ["parent_id", "workspace_id"]

