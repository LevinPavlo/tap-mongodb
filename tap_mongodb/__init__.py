#!/usr/bin/env python3
from tap_mongodb.connection import get_client
from tap_mongodb.discover import do_discover
from tap_mongodb.sync import do_sync
import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.incremental as incremental
import singer
from singer import utils
import json
import sys

LOGGER = singer.get_logger()
ARGS = utils.parse_args([])
CONFIG = ARGS.config


def main_impl():
    client = get_client(ARGS, CONFIG)
    args = client.args
    config = client.config
    common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = \
        (config.get('include_schemas_in_destination_stream_name') == 'true')

    if args.discover:
        catalog = do_discover(client, config, limit=1000)
        json.dump(catalog, sys.stdout, indent=2)
    else:
        state = args.state or {}
        # merge dictionaries to get selected streams and replication method
        catalog = args.catalog.to_dict()
        rediscovered_catalog = do_discover(client, config, limit=None)
        full_catalog = catalog
        try:
            full_catalog = get_full_catalog(catalog, rediscovered_catalog)
        except Exception as e:
            LOGGER.info(e)
            pass
        do_sync(client, full_catalog, state)


def get_full_catalog(sample_catalog, rediscovered_catalog):
    """
        Update rediscovered catalog with 'selected' and 'replication-method' info
    """
    for rediscovered_stream in rediscovered_catalog.get("streams", []):

        print("Print rediscovered stream before change", rediscovered_stream)

        matched_streams = [s for s in sample_catalog["streams"] if
                           s["tap_stream_id"] == rediscovered_stream["tap_stream_id"]]
        if matched_streams:
            metadata = [m for m in matched_streams[0].get("metadata", [])]
            replication_method_list = [r["metadata"].get("replication-method", False) for r in metadata if
                                       r["metadata"].get("replication-method", False)]
            replication_method = replication_method_list and replication_method_list[0] or "FULL_TABLE"
            selected_streams = [stream_name["stream"] for stream_name in matched_streams]
            for rediscovered_metadata in rediscovered_stream["metadata"]:
                if rediscovered_metadata["metadata"].get("table-key-properties", False):
                    # insert replication method
                    rediscovered_metadata["metadata"]["replication-method"] = replication_method
                if rediscovered_stream["stream"] in selected_streams:
                    rediscovered_metadata["metadata"]["selected"] = True

                print("Print rediscovered stream after change", rediscovered_stream)
    return rediscovered_catalog


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
