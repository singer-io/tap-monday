from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Updates(IncrementalStream):
    tap_stream_id = "updates"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "data.updates"
    children = ["assets", "reply"]
    root_field = "updates(limit:{limit}, page:{page})"
    page_size = 100
    pagination_supported = True
    common_asset_fields = [
        "id", "name", "created_at", "file_extension", "file_size",
        "original_geometry", "public_url", "url", "url_thumbnail"
    ]
    extra_fields = {
        "assets": common_asset_fields,
        "assets.uploaded_by": ["id"],
        "replies": [
            "id", "body", "created_at", "edited_at", "kind",
            "text_body", "updated_at", "creator_id"
        ],
        "replies.assets": common_asset_fields,
        "replies.assets.uploaded_by": ["id"],
        "replies.likes": ["id", "creator_id", "reaction_type", "created_at", "updated_at"],
        "replies.viewers": ["user_id", "medium"]
    }

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """
        A wrapper for singer.get_bookmark to handle compatibility with both
        parent and child stream bookmark values, excluding full-table replication children.
        """
        min_parent_bookmark = super().get_bookmark(state, stream) if self.is_selected() else ""
        for child in self.child_to_sync:
            if not child.is_selected():
                continue
            if getattr(child, "replication_method", "").upper() == "FULL_TABLE":
                continue  # Skip full-table replication children

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
        page = kwargs.get("page", 1)
        root_field = self.root_field.format(limit=self.page_size, page=page)
        graphql_query = self.get_graphql_query(root_field)
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

