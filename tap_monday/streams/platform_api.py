from typing import Dict, Any
from singer import get_logger
from tap_monday.streams.abstracts import IncrementalStream
from tap_monday.exceptions import MondayForbiddenError, MondayGraphQLInternalError

LOGGER = get_logger()


class PlatformApi(IncrementalStream):
    tap_stream_id = "platform_api"
    key_properties = ["last_updated"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_updated"]
    bookmark_value = None
    data_key = "data.platform_api"
    root_field = "platform_api"
    excluded_fields = ["last_updated"]
    extra_fields = {
        "daily_analytics.last_updated": []
    }

    def check_access(self):
        """platform_api is always present; only individual fields are plan-gated.
        Field-level access is handled by prune_inaccessible_fields(), so skip
        the stream-level probe here."""
        return True

    def prune_inaccessible_fields(self, schema: dict, field_metadata: list) -> None:
        """Probe whether daily_limit is accessible for this account's Monday plan.
        If the field is not available (MondayForbiddenError or MondayGraphQLInternalError),
        remove it from the catalog schema and its metadata entry, and log a warning
        so sync can continue without it.
        """
        import json as _json
        url = self.get_url_endpoint()
        self.update_params()
        body = _json.dumps({"query": "query { platform_api { daily_limit { total } } }"})
        try:
            self.client.probe_request(self.http_method, url, self.params, self.headers, body=body)
        except (MondayForbiddenError, MondayGraphQLInternalError):
            LOGGER.warning(
                "Stream 'platform_api': field 'daily_limit' is not supported for this "
                "Monday plan removing it from the catalog. Sync will continue without it."
            )
            schema.get("properties", {}).pop("daily_limit", None)
            field_metadata[:] = [
                entry for entry in field_metadata
                if entry.get("breadcrumb") not in (
                    ["properties", "daily_limit"],
                    ("properties", "daily_limit"),
                )
            ]

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["last_updated"] = record["daily_analytics"]["last_updated"]
        return record

