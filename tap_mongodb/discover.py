# Discovery method
import json
import logging

import sys
from concurrent.futures import ThreadPoolExecutor

from pymongo.collection import Collection
from bson import errors

import singer
from singer import metadata
from pymongo_schema import extract
import time
LOGGER = singer.get_logger()

IGNORE_DBS = ['system', 'local', 'config']
ROLES_WITHOUT_FIND_PRIVILEGES = {
    'dbAdmin',
    'userAdmin',
    'clusterAdmin',
    'clusterManager',
    'clusterMonitor',
    'hostManager',
    'restore'
}
ROLES_WITH_FIND_PRIVILEGES = {
    'read',
    'readWrite',
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'dbOwner',
    'backup',
    'root'
}
ROLES_WITH_ALL_DB_FIND_PRIVILEGES = {
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'root'
}
STEP_LIMIT = 100000


def do_discover(client, config):
    streams = []

    db_name = config.get("database")
    filter_collections = config.get("filter_collections", [])
    if db_name == "admin":
        databases = get_databases(client, config)
    else:
        databases = [db_name]

    for db_name in databases:
        # pylint: disable=invalid-name
        db = client[db_name]

        collection_names = db.list_collection_names()
        for collection_name in collection_names:
            if collection_name.startswith("system.") or (
                    filter_collections and collection_name not in filter_collections):
                continue

            collection = db[collection_name]
            is_view = collection.options().get('viewOn') is not None
            # TODO: Add support for views
            if is_view:
                continue

            LOGGER.info("Getting collection info for db: %s, collection: %s",
                        db_name, collection_name)
            stream = produce_collection_schema(collection, client)
            if stream is not None:
                streams.append(stream)
    json.dump({'streams': streams}, sys.stdout, indent=2)


def get_databases(client, config):
    roles = get_roles(client, config)
    LOGGER.info('Roles: %s', roles)

    can_read_all = len([r for r in roles if r['role'] in ROLES_WITH_ALL_DB_FIND_PRIVILEGES]) > 0

    if can_read_all:
        db_names = [d for d in client.list_database_names() if d not in IGNORE_DBS]
    else:
        db_names = [r['db'] for r in roles if r['db'] not in IGNORE_DBS]
    LOGGER.info('Datbases: %s', db_names)
    return db_names


def get_roles(client, config):
    # usersInfo Command returns object in shape:
    # {
    #     <some_other_keys>
    #     'users': [
    #         {
    #             '_id': <auth_db>.<user>,
    #             'db': <auth_db>,
    #             'mechanisms': ['SCRAM-SHA-1', 'SCRAM-SHA-256'],
    #             'roles': [{'db': 'admin', 'role': 'readWriteAnyDatabase'},
    #                       {'db': 'local', 'role': 'read'}],
    #             'user': <user>,
    #             'userId': <userId>
    #         }
    #     ]
    # }
    user_info = client[config['database']].command({'usersInfo': config['user']})

    users = [u for u in user_info.get('users') if u.get('user') == config['user']]
    if len(users) != 1:
        LOGGER.warning('Could not find any users for %s', config['user'])
        return []

    roles = []
    for role in users[0].get('roles', []):
        if role.get('role') is None:
            continue

        role_name = role['role']
        # roles without find privileges
        if role_name in ROLES_WITHOUT_FIND_PRIVILEGES:
            continue

        # roles with find privileges
        if role_name in ROLES_WITH_FIND_PRIVILEGES:
            if role.get('db'):
                roles.append(role)

        # for custom roles, get the "sub-roles"
        else:
            role_info_list = client[config['database']].command(
                {'rolesInfo': {'role': role_name, 'db': config['database']}})
            role_info = [r for r in role_info_list.get('roles', []) if r['role'] == role_name]
            if len(role_info) != 1:
                continue
            for sub_role in role_info[0].get('roles', []):
                if sub_role.get('role') in ROLES_WITH_FIND_PRIVILEGES:
                    if sub_role.get('db'):
                        roles.append(sub_role)
    return roles


def build_schema_for_type(type):
    # If it's the _id
    if type == 'oid':
        return {
            "inclusion": "automatic",
            "type": "string"
        }

    if type == 'date':
        return {
            "inclusion": "available",
            "type": ["null", "string"],
            "format": "date-time"
        }

    if type == 'ARRAY':
        return {
            "inclusion": "available",
            "type": ["null", "array"],
        }

    if type == 'OBJECT':
        return {
            "inclusion": "available",
            "type": ["null", "object"],
        }

    if type == 'float':
        return {
            "inclusion": "available",
            "type": "number",
        }

    if type == "general_scalar":
        return {
            "inclusion": "available",
            "type": ["null", "mixed"],

        }

    return {
        "inclusion": "available",
        "type": type
    }


def build_schema_for_level(properties):
    schema_properties = {}

    for propertyName, propertyInfo in properties.items():
        property_type = propertyInfo['type']

        # Add the field to the schema
        property_schema = build_schema_for_type(property_type)

        # If we have an object we need to build the schema inside
        if property_type == 'OBJECT':
            property_schema['properties'] = build_schema_for_level(propertyInfo['object'])

        schema_properties[propertyName] = property_schema

    return schema_properties


def produce_collection_schema(collection: Collection, client):
    collection_name = collection.name
    collection_db_name = collection.database.name

    is_view = collection.options().get('viewOn') is not None

    # Analyze and build schema recursively
    # schema = extract_pymongo_client_schema(client, collection_names=collection_name)
    try:
        schema = _fault_tolerant_extract_collection_schema(collection, sample_size=None)
        """
        TODO: 
        without sample_size it always downloads all data to extract the schema,
        but with it might not get types for all fields and fail writing

        maybe it should load the schema with a sample_size, but in the actual import build the
        schema message out of all data?
        """
    except errors.InvalidBSON as e:
        logging.warning("ignored db {}.{} due to BSON error: {}".format(
            collection_db_name,
            collection_name,
            str(e)
        ))
        return None
    extracted_properties = schema['object']
    schema_properties = build_schema_for_level(extracted_properties)

    propertiesBreadcrumb = []

    for k in schema_properties:
        propertiesBreadcrumb.append({
            "breadcrumb": ["properties", k],
            "metadata": {"selected-by-default": True}
        })

    mdata = {}
    mdata = metadata.write(mdata, (), 'table-key-properties', ['_id'])
    mdata = metadata.write(mdata, (), 'database-name', collection_db_name)
    mdata = metadata.write(mdata, (), 'row-count', collection.estimated_document_count())
    mdata = metadata.write(mdata, (), 'is-view', is_view)

    # write valid-replication-key metadata by finding fields that have indexes on them.
    # cannot get indexes for views -- NB: This means no key-based incremental for views?
    if not is_view:
        valid_replication_keys = []
        coll_indexes = collection.index_information()
        # index_information() returns a map of index_name -> index_information
        for _, index_info in coll_indexes.items():
            # we don't support compound indexes
            if len(index_info.get('key')) == 1:
                index_field_info = index_info.get('key')[0]
                # index_field_info is a tuple of (field_name, sort_direction)
                if index_field_info:
                    valid_replication_keys.append(index_field_info[0])

        if valid_replication_keys:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', valid_replication_keys)

    return {
        'table_name': collection_name,
        'stream': collection_name,
        'metadata': metadata.to_list(mdata) + propertiesBreadcrumb,
        'tap_stream_id': "{}-{}".format(collection_db_name, collection_name),
        'schema': {
            'type': 'object',
            'properties': schema_properties
        }
    }


def _fault_tolerant_extract_collection_schema(collection: Collection, sample_size: int = None):
    """
    @see extract.extract_collection_schema - but catches InvalidBSON errors

    multithreads scan documents in slices containing params:
    no_cursor_timeout - relates to idle time, which does not contribute towards its processing time
    allow_partial_results - instead of an error returns partial results if some shards are down
    skip - from the start to this index
    limit - number of documents to return
    max_time_ms (ex. 5000 ms - 5s) - relates to processing time as a limit for the query operation
    """
    logger = logging.getLogger()
    collection_schema = {
        'count': 0,
        "object": extract.init_empty_object_schema()
    }

    # count is DEPRECATED -> could use count_documents({}), but is very slow
    document_count = collection.estimated_document_count()
    collection_schema['count'] = document_count

    start_time = time.time()
    steps = int(round(document_count / STEP_LIMIT)) + 1
    logger.info('Total steps - %s', steps)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for i in range(steps):
            start = i * STEP_LIMIT
            cursors = collection.find(no_cursor_timeout=True, allow_partial_results=True, skip=start, limit=STEP_LIMIT,
                                      max_time_ms=5000)
            executor.submit(scan_documents, cursors, collection_schema, STEP_LIMIT, i, steps, document_count,
                            collection.name)

    end_time = time.time() - start_time
    logger.info('Collection %s scanned for - %s', collection.name, int(round(end_time, 2)))

    logger.info('Finished scanning documents of collection %s', collection.name)
    extract.post_process_schema(collection_schema)
    collection_schema = extract.recursive_default_to_regular_dict(collection_schema)
    return collection_schema


def scan_documents(cursors, collection_schema, limit, step, steps, total, collection_name):
    LOGGER.info('Collection %s - Scanning documents: %s/%s - steps %s/%s',
                collection_name, limit * step, total, step, steps)
    try:
        process_cursor(cursors, collection_schema['object'])
        LOGGER.info('Collection %s - Scanned documents: %s/%s - steps %s/%s',
                    collection_name, limit * step, total, step, steps)
    except Exception as e:
        # cursor might be not found for different reasons (time out, live change, etc)
        LOGGER.info('Error exception: %s', e)
        pass


def process_cursor(documents, schema_object):
    i = 0
    while True:
        try:
            # Iterate over documents per step
            document = next(documents)  # while documents.hasNext()
        except StopIteration:
            break
        except errors.InvalidBSON as err:
            LOGGER.warning("ignore invalid record: {}".format(str(err)))
            continue

        process_document(document, schema_object)
        i += 1
    # cursors with no_cursor_timeout turned on are properly closed
    documents.close()


def process_document(document, schema_object):
    extract.add_document_to_object_schema(document, schema_object)