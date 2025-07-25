from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Reply(IncrementalStream):
    tap_stream_id = "reply"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "reply"
    parent = "updates"
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value

