from typing import Dict, Any
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Boards(IncrementalStream):
    tap_stream_id = "boards"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "data.boards"
    children = ["board_activity_logs", "board_columns", "board_groups", "board_items", "board_views"]
    root_field = "boards(limit:{limit}, page:{page})"
    page_size = 300
    pagination_supported = True
    object_to_id = {"creator": "creator", "top_group": "top_group"}
    extra_fields = {
        "creator": ["id"],
        "top_group": ["id"]
    }
    excluded_fields = ['creator_id', 'top_group_id']

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        min_parent_bookmark = super().get_bookmark(state, stream) if self.is_selected() else ""
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
        page = kwargs.get("page", 1)
        root_field = self.root_field.format(limit=self.page_size, page=page)
        graphql_query = self.get_graphql_query(root_field)
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

