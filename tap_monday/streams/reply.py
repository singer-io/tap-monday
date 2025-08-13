from typing import Dict, Any
from singer import get_bookmark, get_logger, Transformer, write_record, metrics
from tap_monday.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Reply(IncrementalStream):
    tap_stream_id = "reply"
    key_properties = ["id", "update_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "reply"
    parent = "updates"
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value

    def extract_replies(self, data):
        all_replies = []
        for reply in data.get("replies", []):
            all_replies.append(reply)
        return all_replies

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["update_id"] = parent_record.get("id")
        return record

    def sync(self, state: Dict, transformer: Transformer, parent_obj: Dict = None) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.extract_replies(parent_obj):
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

