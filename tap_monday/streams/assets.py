from typing import Dict
from singer import get_logger, write_record, Transformer, metrics
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Assets(FullTableStream):
    tap_stream_id = "assets"
    key_properties = ["id", "update_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "assets"
    parent = "updates"
    object_to_id = {"uploaded_by": "uploaded_by"}

    def extract_assets(self, data):
        """
        Extracts all assets from the given data, including assets in top-level
        and nested replies.
        """
        all_assets = []
        for asset in data.get("assets", []):
            all_assets.append(asset)
        for reply in data.get("replies", []):
            for asset in reply.get("assets", []):
                all_assets.append(asset)
        return all_assets

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["update_id"] = parent_record.get("id")
        return record

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.extract_assets(parent_obj):
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

