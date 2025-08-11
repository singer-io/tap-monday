from typing import Dict, Any, List
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class BoardItems(IncrementalStream):
    tap_stream_id = "board_items"
    key_properties = ["id", "board_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "data.boards"
    parent = "boards"
    bookmark_value = None
    children = ["column_values"]
    object_to_id = {"creator": "creator", "group": "group", "parent_item": "parent_item"}
    root_field = "boards (ids: {ids}) {{ items_page {{items "
    extra_fields = {
        "creator": ["id"],
        "group": ["id"],
        "parent_item": ["id"]
        }
    excluded_fields = ["creator_id", "board_id", "group_id", "parent_item_id"]

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = super().get_bookmark(state, stream) if self.is_selected() else None
        for child in self.child_to_sync:
            if child.is_selected():
                bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
                child_bookmark = super().get_bookmark(state, child.tap_stream_id, key=bookmark_key)
                if min_parent_bookmark:
                    min_parent_bookmark = min(min_parent_bookmark, child_bookmark)
                else:
                    min_parent_bookmark = child_bookmark

        return min_parent_bookmark

    def write_bookmark(self, state: Dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            if child.is_selected():
                bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
                super().write_bookmark(state, child.tap_stream_id, key=bookmark_key, value=value)

        return state

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        root_field = self.root_field.format(ids=parent_obj["id"])
        graphql_query = self.get_graphql_query(root_field) + "}}"
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["board_id"] = parent_record["id"]
        return record

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Custom parsing for streams that return data[0]['columns']."""
        return raw_data[0].get("items_page").get("items", []) if raw_data else []

