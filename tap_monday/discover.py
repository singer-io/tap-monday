import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_monday.schema import get_schemas
from tap_monday.streams import STREAMS
from tap_monday.exceptions import MondayForbiddenError, MondayInternalServerError

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, stream_cls.parent,
            )
            schemas.pop(name)
            field_metadata.pop(name)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Note: check_access() always returns None for child streams, so this loop
    effectively identifies only inaccessible parent streams by design.
    Child stream removal is handled separately by _prune_inaccessible_children().
    Raises MondayForbiddenError if no parent streams are accessible.
    """
    # Map stream_name -> the exception returned by check_access (None means accessible).
    access_results = {
        stream_name: stream_obj(client=client).check_access()
        for stream_name, stream_obj in STREAMS.items()
        if stream_name in schemas
    }
    inaccessible_streams = [
        name for name, exc in access_results.items() if exc is not None
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if inaccessible_streams:
        total_parent_streams = len([s for s in STREAMS.values() if not s.parent])
        if len(inaccessible_streams) == total_parent_streams:
            # Determine the dominant failure type to build an accurate message.
            causes = [access_results[n] for n in inaccessible_streams]
            has_500 = any(isinstance(c, MondayInternalServerError) for c in causes)
            has_403 = any(isinstance(c, MondayForbiddenError) for c in causes)
            if has_500 and not has_403:
                raise MondayInternalServerError(
                    "HTTP-error-code: 500, Error: All streams returned a server error during access checks. "
                    "This may indicate a missing app installation or a platform-side issue."
                )
            raise MondayForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog

