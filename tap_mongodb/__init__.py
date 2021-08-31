#!/usr/bin/env python3
import ssl
import pymongo
import singer
from singer import utils

from tap_mongodb.discover import do_discover
from tap_mongodb.sync import do_sync
import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.incremental as incremental

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]


def main_impl():
    # Check if we use DNS string or not
    args = utils.parse_args([])
    config = args.config

    if 'connection_uri' in config.keys():
        # Connect using the DNS string
        parsedUri = pymongo.uri_parser.parse_uri(config['connection_uri'])
        config['username'] = parsedUri['username']
        config['password'] = parsedUri['password']
        config['authSource'] = parsedUri['options']['authsource']
        config['user'] = parsedUri['username']
        config['database'] = parsedUri.get('database')

        if config.get("database") is None:
            config["database"] = "admin"

        client = pymongo.MongoClient(config['connection_uri'])
        LOGGER.info('Connected to MongoDB host: %s, version: %s',
                    config['connection_uri'].replace(config.get("password"), "********"),
                    client.server_info().get('version', 'unknown'))
    else:
        # Connect using the connection parameters
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        config = args.config

        # Default SSL verify mode to true, give option to disable
        verify_mode = config.get('verify_mode', 'true') == 'true'
        use_ssl = config.get('ssl') == 'true'

        connection_params = {"host": config['host'],
                             "port": int(config['port']),
                             "username": config.get('user', None),
                             "password": config.get('password', None),
                             "authSource": config['database'],
                             "ssl": use_ssl,
                             "replicaset": config.get('replica_set', None),
                             "readPreference": 'secondaryPreferred'}

        # NB: "ssl_cert_reqs" must ONLY be supplied if `SSL` is true.
        if not verify_mode and use_ssl:
            connection_params["ssl_cert_reqs"] = ssl.CERT_NONE
        client = pymongo.MongoClient(**connection_params)

        LOGGER.info('Connected to MongoDB host: %s, version: %s',
                    config['host'],
                    client.server_info().get('version', 'unknown'))

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
