from typing import Dict, Any
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream, ParentChildBookmarkMixin

LOGGER = get_logger()


class Boards(ParentChildBookmarkMixin, IncrementalStream):
    tap_stream_id = "boards"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "data.boards"
    children = ["board_activity_logs", "board_columns", "board_groups", "board_items", "board_views"]
    root_field = "boards(limit:{limit}, page:{page})"
    page_size = 200
    pagination_supported = True
    object_to_id = {"creator": "creator", "top_group": "top_group"}
    extra_fields = {
        "creator": ["id"],
        "top_group": ["id"]
    }
    excluded_fields = ['creator_id', 'top_group_id']

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        page = kwargs.get("page", 1)
        root_field = self.root_field.format(limit=self.page_size, page=page)
        graphql_query = self.get_graphql_query(root_field)
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

