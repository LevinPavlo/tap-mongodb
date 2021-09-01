#!/usr/bin/env python3
import ssl
import pymongo
import singer
from singer import utils

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]


def get_client(args, config):
    try:
        return check_connection(args, config)
    except Exception as e:
        raise Exception("Could not connect to client! ", e)


def check_connection(args, config):
    # self.session = pymongo.MongoClient.start_session(causal_consistency=True)
    if 'connection_uri' in config.keys():
        # Connect using the DNS string
        parsed_uri = pymongo.uri_parser.parse_uri(config['connection_uri'])
        config.update({
            "username": parsed_uri['username'],
            "password": parsed_uri['password'],
            "authSource": parsed_uri['options']['authsource'],
            "user": parsed_uri['username'],
            "database": parsed_uri.get('database', 'admin')
        })

        client = pymongo.MongoClient(config['connection_uri'])
        LOGGER.info('Connected to MongoDB host: %s, version: %s',
                    config['connection_uri'].replace(config.get("password"), "********"),
                    client.server_info().get('version', 'unknown'))
        client.args = args
        client.config = config
        return client
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
        client.args = args
        client.config = config
        return client


