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
    page_size = 200
    root_field = """boards(ids: {ids}, limit:1, page:1) {{ activity_logs(limit:{limit}, page:{page})"""
    excluded_fields = ["board_id"]
    pagination_supported = True

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
        page = kwargs.get("page", 1)
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError(f"{self.tap_stream_id} - parent_obj must be provided with an 'id' key.")
        root_field = self.root_field.format(ids=parent_obj["id"], limit=self.page_size, page=page)
        graphql_query = self.get_graphql_query(root_field) + "}"
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream.
        The timestamps(created_at) returned by this field are formattedas UNIX time with 17 digits.
        To convert the timestamp to UNIX time in milliseconds, dividing the 17-digit value by 10,000.
        After that Transformer object take care of 13 digit UNIX time in milliseconds.
        """
        record = super().modify_object(record, parent_record)
        # The 17-digit created_at timestamp should be divided by 10000 to convert it to a
        # standard 13-digit UNIX time in milliseconds.
        record["created_at"] = int(record["created_at"])//10000
        record["board_id"] = parent_record.get("id")
        return record

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Custom parsing for streams that return data[0]['activity_logs']."""
        return raw_data[0].get("activity_logs", []) if raw_data else []

