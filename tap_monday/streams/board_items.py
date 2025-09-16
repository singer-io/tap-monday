from typing import Dict, Any, List
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class BoardItems(IncrementalStream):
    tap_stream_id = "board_items"
    key_properties = ["id", "board_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "data"
    parent = "boards"
    bookmark_value = None
    children = ["column_values"]
    object_to_id = {"creator": "creator", "group": "group", "parent_item": "parent_item"}
    page_size = 20
    pagination_supported = True
    root_field = "boards (ids: {ids}) {{ items_page(limit: {limit}) {{cursor items "
    root_field_pagination_query = """next_items_page(limit: {limit}, cursor: "{cursor}") {{cursor items """
    extra_fields = {
        "creator": ["id"],
        "group": ["id"],
        "parent_item": ["id"]
        }
    excluded_fields = ["creator_id", "board_id", "group_id", "parent_item_id"]

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value

    def write_bookmark(self, state: Dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            if not child.is_selected():
                continue

            if getattr(child, "replication_method", "").upper() == "FULL_TABLE":
                continue  # Skip full_table children

            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(state, child.tap_stream_id, key=bookmark_key, value=value)
        return state

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        if self.cursor:
            root_field = self.root_field_pagination_query.format(limit=self.page_size, cursor=self.cursor)
            graphql_query = self.get_graphql_query(root_field) + "}"
        else:
            if not parent_obj or 'id' not in parent_obj:
                raise ValueError(f"{self.tap_stream_id} - parent_obj must be provided with an 'id' key.")
            root_field = self.root_field.format(ids=parent_obj["id"], limit=self.page_size)
            graphql_query = self.get_graphql_query(root_field) + "}}"
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["board_id"] = parent_record.get("id")
        return record

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Custom parsing for streams that return data[0]['columns']."""
        items_page = dict()
        if self.cursor:
            items_page = raw_data[0].get("next_items_page", {}) if raw_data else {}
        else:
            items_page = raw_data[0].get("boards", [])[0].get("items_page", {}) if raw_data else {}
        self.cursor = items_page.get("cursor")
        return items_page.get("items", [])

    def update_pagination_key(self, raw_records, parent_record, next_page):
        """Updates the pagination key for fetching the next page of results."""
        if not self.pagination_supported or not self.cursor:
            return None
        next_page += 1
        self.update_data_payload(self._graphql_query, parent_record)
        return next_page

