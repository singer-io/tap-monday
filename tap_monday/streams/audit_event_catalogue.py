from typing import Dict, Iterator, List
from singer import get_logger
from tap_monday.streams.abstracts import FullTableStream

LOGGER = get_logger()


class AuditEventCatalogue(FullTableStream):
    tap_stream_id = "audit_event_catalogue"
    key_properties = ["name"]
    replication_method = "FULL_TABLE"
    data_key = "data.audit_event_catalogue"

