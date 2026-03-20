from typing import Dict, Any, List, Tuple
from singer import get_logger, metrics, write_record, Transformer
from tap_monday.streams.abstracts import IncrementalStream
from tap_monday.exceptions import MondayCursorExpiredError

# Maximum number of times a single board's query will be restarted after a
# cursor expiry before aborting.  Prevents an infinite loop in the unlikely
# case where the API consistently expires cursors for a given board.
MAX_CURSOR_RETRIES = 5

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

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Tuple[int, Dict]:
        """Override sync to gracefully handle Monday.com cursor expiration.

        When the API returns a ``CursorException`` mid-pagination the current
        cursor is discarded, the bookmark filter is tightened to the latest
        ``updated_at`` value seen so far (to reduce duplicates), and the query
        is restarted from the beginning for the current board.

        At most ``MAX_CURSOR_RETRIES`` restarts are allowed per board; if the
        limit is exceeded the error is re-raised so the sync does not loop
        indefinitely.
        """
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self._graphql_query = self.get_graphql_query(self.root_field)
        self.update_data_payload(graphql_query=self._graphql_query, parent_obj=parent_obj)
        # After a cursor-expiry restart we use a strict (>) filter so that the
        # record at exactly the tightened bookmark timestamp is not re-emitted.
        # Only enabled once we have actually advanced past the original bookmark;
        # if no records were seen before expiry the filter would incorrectly skip
        # records whose updated_at equals the original bookmark.
        strict_bookmark_filter = False
        restart_count = 0

        with metrics.record_counter(self.tap_stream_id) as counter:
            while True:
                try:
                    for record in self.get_records(parent_obj):
                        record = self.modify_object(record, parent_obj)
                        transformed_record = transformer.transform(
                            record, self.schema, self.metadata
                        )
                        record_timestamp = transformed_record[self.replication_keys[0]]
                        passes_filter = (
                            record_timestamp > bookmark_date
                            if strict_bookmark_filter
                            else record_timestamp >= bookmark_date
                        )
                        if passes_filter:
                            if self.is_selected():
                                write_record(self.tap_stream_id, transformed_record)
                                counter.increment()
                            current_max_bookmark_date = max(
                                current_max_bookmark_date, record_timestamp
                            )
                            for child in self.child_to_sync:
                                child.sync(
                                    state=state,
                                    transformer=transformer,
                                    parent_obj=record,
                                )
                    break  # all pages fetched successfully

                except MondayCursorExpiredError:
                    restart_count += 1
                    if restart_count > MAX_CURSOR_RETRIES:
                        raise RuntimeError(
                            f"Cursor expired {restart_count} times for stream "
                            f"'{self.tap_stream_id}' on board "
                            f"'{parent_obj.get('id') if parent_obj else 'unknown'}'. "
                            "Aborting to prevent an infinite loop."
                        )
                    LOGGER.warning(
                        "Cursor expired for stream '%s' while paginating board '%s' "
                        "(restart %d/%d). Restarting query using latest bookmark: %s",
                        self.tap_stream_id,
                        parent_obj.get("id") if parent_obj else "unknown",
                        restart_count,
                        MAX_CURSOR_RETRIES,
                        current_max_bookmark_date,
                    )
                    # Only tighten the bookmark filter when progress was made
                    # (i.e. we actually advanced past the original bookmark).
                    # If the cursor expired before any record was written we
                    # keep the original bookmark so records at that exact
                    # timestamp are not lost.
                    if current_max_bookmark_date > bookmark_date:
                        bookmark_date = current_max_bookmark_date
                        self.bookmark_value = current_max_bookmark_date
                        strict_bookmark_filter = True
                    # Reset cursor so the next iteration starts a fresh query
                    self.cursor = None
                    self.update_data_payload(self._graphql_query, parent_obj)

        state = self.write_bookmark(
            state, self.tap_stream_id, value=current_max_bookmark_date
        )
        return counter.value, state

