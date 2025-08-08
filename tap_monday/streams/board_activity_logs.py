from typing import Dict, Any, List
from singer import get_logger

from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class BoardActivityLogs(IncrementalStream):
    tap_stream_id = "board_activity_logs"
    key_properties = ["id", "board_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["created_at"]
    data_key = "data.boards"
    parent = "boards"
    bookmark_value = None
    root_field = "boards(ids: {ids}) {{ activity_logs"
    excluded_fields = ["board_id"]

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        root_field = self.root_field.format(ids=parent_obj["id"])
        graphql_query = self.get_graphql_query(root_field) + "}"
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["created_at"] = int(record["created_at"])//10000
        record["board_id"] = parent_record["id"]
        return record

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Custom parsing for streams that return data[0]['activity_logs']."""
        return raw_data[0].get("activity_logs", []) if raw_data else []

