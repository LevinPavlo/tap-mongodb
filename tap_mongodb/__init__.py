#!/usr/bin/env python3
import singer
from tap_mongodb.connection import get_client

from tap_mongodb.discover import do_discover
from tap_mongodb.sync import do_sync
import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.incremental as incremental
from singer import utils

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
        do_discover(client, config)
    elif args.catalog:
        state = args.state or {}
        do_sync(client, args.catalog.to_dict(), state)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
