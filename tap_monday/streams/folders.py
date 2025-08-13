from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Folders(FullTableStream):
    tap_stream_id = "folders"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.folders"
    root_field = "folders(limit:{limit}, page:{page})"
    page_size = 100
    pagination_supported = True
    object_to_id = {"parent": "parent", "workspace": "workspace"}
    extra_fields = {
        "parent": ["id"],
        "workspace": ["id"]
    }
    excluded_fields = ["parent_id", "workspace_id"]

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """ Update JSON body for GraphQL API. Injects query string if provided."""
        page = kwargs.get("page", 1)
        root_field = self.root_field.format(limit=self.page_size, page=page)
        graphql_query = self.get_graphql_query(root_field)
        super().update_data_payload(graphql_query=graphql_query, parent_obj=parent_obj, **kwargs)

