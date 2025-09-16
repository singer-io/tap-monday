from typing import Dict
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "data.users"
    object_to_id = {"account": "account"}
    root_field = "users(limit:{limit}, page:{page})"
    page_size = 200
    pagination_supported = True
    extra_fields = {
        "account": ["id", ],
        }
    excluded_fields = ["account_id"]

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        page = kwargs.get("page", 1)
        root_field = self.root_field.format(limit=self.page_size, page=page)
        graphql_query = self.get_graphql_query(root_field)
        super().update_data_payload(
            graphql_query=graphql_query,
            parent_obj=parent_obj,
            **kwargs
        )

