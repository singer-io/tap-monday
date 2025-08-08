from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.users"
    object_to_id = {"account": "account"}
    root_field = "users"
    extra_fields = {
        "account": ["id", ],
        }
    excluded_fields = ["account_id"]

