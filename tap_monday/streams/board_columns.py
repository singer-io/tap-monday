from typing import Dict, List, Any
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class BoardColumns(FullTableStream):
    tap_stream_id = "board_columns"
    key_properties = ["id", "board_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.boards"
    parent = "boards"
    root_field = "boards(ids: {ids}) {{ columns"
    excluded_fields = ["board_id"]

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
        record["board_id"] = parent_record.get("id")
        return record

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Custom parsing for streams that return data[0]['columns']."""
        return raw_data[0].get("columns", []) if raw_data else []

