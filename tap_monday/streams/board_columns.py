from typing import Dict, List, Any
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class BoardColumns(IncrementalStream):
    tap_stream_id = "board_columns"
    key_properties = ["id", "board_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "data.boards"
    parent = "boards"
    root_field = "boards(ids: {ids}) {{ columns"
    excluded_fields = ["board_id", "updated_at"]

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError(f"{self.tap_stream_id} - parent_obj must be provided with an 'id' key.")
        root_field = self.root_field.format(ids=parent_obj["id"])
        graphql_query = self.get_graphql_query(root_field) + "}"
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["board_id"] = parent_record.get("id")
        record["updated_at"] = parent_record.get("updated_at")
        return record

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Custom parsing for streams that return data[0]['columns']."""
        return raw_data[0].get("columns", []) if raw_data else []

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        return state
