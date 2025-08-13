from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Docs(FullTableStream):
    tap_stream_id = "docs"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.docs"
    object_to_id = {"created_by": "creator"}
    root_field = "docs"
    extra_fields = {
        "created_by": ["id"]
        }
    excluded_fields = ["creator_id"]

