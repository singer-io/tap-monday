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
        # IDs of records already emitted whose updated_at equals the current
        # boundary timestamp (current_max_bookmark_date).  On a cursor-expiry
        # restart the filter stays inclusive (>=) so that peer records sharing
        # that timestamp are not lost; this set is used to de-duplicate only
        # the records that were already written in a previous attempt.
        # The set is cleared automatically whenever current_max_bookmark_date
        # advances, and is carried across restarts so de-dupe remains accurate
        # even after multiple consecutive cursor expiries.
        emitted_ids_at_max: set = set()
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
                        if record_timestamp < bookmark_date:
                            continue
                        # Skip records that were already emitted in a previous
                        # attempt at the boundary timestamp to avoid duplicates
                        # while keeping the filter inclusive so that not-yet-
                        # emitted peer records at the same timestamp are not lost.
                        if (
                            record_timestamp == bookmark_date
                            and record["id"] in emitted_ids_at_max
                        ):
                            continue
                        if self.is_selected():
                            write_record(self.tap_stream_id, transformed_record)
                            counter.increment()
                        # Advance the boundary tracker; clear the de-dupe set
                        # whenever we move to a strictly later timestamp.
                        if record_timestamp > current_max_bookmark_date:
                            current_max_bookmark_date = record_timestamp
                            emitted_ids_at_max = set()
                        if record_timestamp == current_max_bookmark_date:
                            emitted_ids_at_max.add(record["id"])
                        for child in self.child_to_sync:
                            try:
                                child.sync(
                                    state=state,
                                    transformer=transformer,
                                    parent_obj=record,
                                )
                            except MondayCursorExpiredError as exc:
                                # A cursor expiry inside a child stream must not
                                # be caught by the parent's restart handler —
                                # doing so would reset the parent board's cursor
                                # when only the child's pagination failed.
                                # Re-raise as RuntimeError so it propagates up
                                # to the caller instead.
                                raise RuntimeError(
                                    f"Cursor expired in child stream "
                                    f"'{child.tap_stream_id}' while syncing "
                                    f"parent '{self.tap_stream_id}' "
                                    f"(board '{parent_obj.get('id') if parent_obj else 'unknown'}'). "
                                    "Child cursor expiry must be handled within "
                                    "the child stream itself."
                                ) from exc
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
                    # Tighten the bookmark to the furthest point reached so that
                    # records already safely in the past are not re-processed.
                    # emitted_ids_at_max is intentionally kept so that records
                    # already emitted at the new bookmark boundary are de-duped
                    # on the next pass without dropping peers at the same timestamp.
                    if current_max_bookmark_date > bookmark_date:
                        bookmark_date = current_max_bookmark_date
                    # Reset cursor so the next iteration starts a fresh query
                    self.cursor = None
                    self.update_data_payload(self._graphql_query, parent_obj)

        state = self.write_bookmark(
            state, self.tap_stream_id, value=current_max_bookmark_date
        )
        return counter.value, state

