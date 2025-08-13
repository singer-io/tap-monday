from abc import ABC, abstractmethod
import json
from typing import Any, Dict, Tuple, Iterator, List
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata
)

LOGGER = get_logger()


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 100
    next_page_key = "page"
    headers = {'Accept': 'application/json'}
    object_to_id = {}
    children = []
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    graphql_query_key = "query"
    root_field = None
    extra_fields = {}
    excluded_fields = []

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}
        self.data_payload = {}
        self.http_method = "POST"

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def get_records(self) -> Iterator:
        """Interacts with api client interaction and pagination."""
        # self.params["page"] = self.page_size
        next_page = 1
        while next_page:
            response = self.client.make_request(
                self.http_method, self.url_endpoint, self.params, self.headers, body=json.dumps(self.data_payload), path=self.path
            )
            raw_records = self.get_dot_path_value(response, self.data_key)
            raw_records = self.parse_raw_records(raw_records)
            next_page = response.get(self.next_page_key)

            # self.params[self.next_page_key] = next_page
            yield from raw_records

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)

    def add_object_to_id(self, record: Dict) -> Dict:
        """
        Adds identifier fields to a record based on nested object mappings.
        """
        for key in self.object_to_id:
            if record[key]:
                record[self.object_to_id[key] + "_id"] = record[key]["id"]
            else:
                record[key + "_id"] = None

        return record

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = self.add_object_to_id(record)
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream
        """
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"

    def update_data_payload(self, graphql_query: str = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update JSON body for GraphQL API. Injects query string if provided.
        """
        if graphql_query:
            self.data_payload[self.graphql_query_key] = graphql_query
        self.data_payload.update(kwargs)

    def get_dot_path_value(self, record: dict, dotted_path: str, default=None):
        """
        Safely retrieve a nested value from a dictionary using a dotted key path.
        This method navigates through a dictionary using a dot-separated string
        (e.g., "user.address.city") to access deeply nested values.
        """
        keys = dotted_path.split(".")
        value = record
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        if isinstance(value, dict):
            return [value]
        elif isinstance(value, list):
            return value

    def parse_raw_records(self, raw_data: Any) -> List[Dict]:
        """Default implementation — override if structure varies."""
        return raw_data or []

    def get_graphql_query(self, root_field: str, indent: int = 1, level: int = 1) -> str:
        """
        Generate a GraphQL query string from JSON schema, including extra fields.
        Supports injecting extra fields even when paths are not in the schema.

        Args:
            root_field (str): Root field String.
            indent (int): Indentation spaces.
            level (int): Starting indentation level.

        Returns:
            str: GraphQL query string
        """
        extra_fields = self.extra_fields or {}
        schema_properties = self.schema.get("properties", {})

        def is_object(schema):
            type = schema.get("type", [])
            return "object" in ([type] if isinstance(type, str) else type)

        def is_array_of_objects(schema):
            type = schema.get("type", [])
            if "array" in ([type] if isinstance(type, str) else type):
                items = schema.get("items", {})
                return is_object(items)
            return False

        def collect_extra_tree():
            tree = {}
            for path, fields in extra_fields.items():
                parts = path.split(".")
                node = tree
                for part in parts:
                    node = node.setdefault(part, {})
                node["_extras"] = fields
            return tree

        def process_properties(properties, depth, parent_path="", extras_branch=None):
            lines = []
            prefix = " " * (indent * depth)
            # Merge keys from schema and extra tree
            schema_keys = set(properties.keys() if properties else [])
            extra_keys = set(extras_branch.keys()) if extras_branch else set()
            all_keys = sorted(schema_keys | extra_keys - {"_extras"})
            all_keys = all_keys = [key for key in all_keys if ((f"{parent_path}.{key}") if parent_path else key) not in self.excluded_fields]

            for key in all_keys:
                full_path = f"{parent_path}.{key}" if parent_path else key
                prop = properties.get(key, {}) if properties else {}
                child_extras = extras_branch.get(key, {}) if extras_branch else {}

                if is_object(prop):
                    nested = process_properties(
                        prop.get("properties", {}),
                        depth,
                        full_path,
                        extras_branch=child_extras
                    )
                    lines.append(f"{prefix}{key} {{{nested}{prefix}}}")
                elif is_array_of_objects(prop):
                    nested = process_properties(
                        prop.get("items", {}).get("properties", {}),
                        depth,
                        full_path,
                        extras_branch=child_extras
                    )
                    lines.append(f"{prefix}{key} {{{nested}{prefix}}}")
                elif child_extras:
                    nested_schema = {}
                    nested = process_properties(
                        nested_schema,
                        depth,
                        full_path,
                        extras_branch=child_extras
                    )
                    if child_extras and "_extras" in child_extras:
                        for extra in child_extras["_extras"]:
                            nested += " " * (indent * (depth)) + extra
                        if nested.strip():
                            lines.append(f"{prefix}{key} {{{nested}{prefix}}}")
                        else:
                            lines.append(f"{prefix}{key}")
                else:
                    lines.append(f"{prefix}{key}")
            return "".join(lines)

        extra_tree = collect_extra_tree()
        inner_body = process_properties(
            schema_properties,
            depth=level,
            parent_path="",
            extras_branch=extra_tree
        )
        if root_field:
            outer_indent = " " * indent * level
            inner_body = f"{outer_indent}{root_field} {{{inner_body}{outer_indent}}}"
        return f"query {{{inner_body}}}"


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""
    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        # self.update_params(updated_since=bookmark_date)
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self._graphql_query = self.get_graphql_query(self.root_field)
        self.update_data_payload(graphql_query=self._graphql_query, parent_obj=parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value, state


class FullTableStream(BaseStream):
    """Base Class for Incremental Stream."""
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self._graphql_query = self.get_graphql_query(self.root_field)
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_data_payload(graphql_query=self._graphql_query, parent_obj=parent_obj)
        # self.update_params(parent_obj=parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected:
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value, state

