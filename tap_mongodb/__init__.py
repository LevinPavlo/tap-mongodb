#!/usr/bin/env python3
from tap_mongodb.connection import get_client
from tap_mongodb.discover import do_discover
from tap_mongodb.sync import do_sync
from tap_mongodb.utils import get_full_catalog
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
        catalog = args.catalog.to_dict()
        # merge dictionaries to get selected streams and replication method
        # full table columns coverage && split parent-child
        rediscovered_catalog = do_discover(client, config, limit=None)
        full_catalog = catalog
        selected_streams = None
        try:
            # some overwrite of selected=True in parent-child. Pass stream_names to restrict
            full_catalog, selected_streams = get_full_catalog(catalog, rediscovered_catalog)
        except Exception as e:
            LOGGER.info(e)
            pass
        do_sync(client, full_catalog, state, selected_streams)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
