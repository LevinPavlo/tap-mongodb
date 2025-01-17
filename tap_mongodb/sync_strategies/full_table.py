#!/usr/bin/env python3
import copy
import datetime
import json
import logging
import time

import bson
import pymongo
import singer
import tap_mongodb.sync_strategies.common as common
from bson import codec_options, errors
from singer import metadata, utils
from singer.transform import SchemaMismatch

LOGGER = singer.get_logger()


def get_max_id_value(collection):
    row = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
    if row:
        return row['_id']

    LOGGER.info("No max id found for collection: collection is likely empty")
    return None


# pylint: disable=too-many-locals,invalid-name,too-many-statements
def sync_collection(client, stream, state, projection):
    tap_stream_id = stream['tap_stream_id']
    LOGGER.info('Starting full table sync for %s', tap_stream_id)

    md_map = metadata.to_map(stream['metadata'])
    database_name = metadata.get(md_map, (), 'database-name')

    db = client[database_name]
    stream_name = metadata.get(md_map, (), 'collection') or stream.get("stream", False)
    collection = db[stream_name]

    #before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None

    # last run was interrupted if there is a last_id_fetched bookmark
    was_interrupted = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched') is not None

    #pick a new table version if last run wasn't interrupted
    if was_interrupted:
        stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    else:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    activate_version_message = singer.ActivateVersionMessage(
        stream=common.calculate_destination_stream_name(stream),
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_message(activate_version_message)

    if singer.get_bookmark(state, stream['tap_stream_id'], 'max_id_value'):
        # There is a bookmark
        max_id_value = singer.get_bookmark(state, stream['tap_stream_id'], 'max_id_value')
        max_id_type = singer.get_bookmark(state, stream['tap_stream_id'], 'max_id_type')
        max_id_value = common.string_to_class(max_id_value, max_id_type)
    else:
        max_id_value = get_max_id_value(collection)

    last_id_fetched = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched')

    if max_id_value:
        # Write the bookmark if max_id_value is defined
        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'max_id_value',
                                      common.class_to_string(max_id_value,
                                                             max_id_value.__class__.__name__))
        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'max_id_type',
                                      max_id_value.__class__.__name__)

    find_filter = {'$lte': max_id_value}
    if last_id_fetched:
        last_id_fetched_type = singer.get_bookmark(state,
                                                   stream['tap_stream_id'],
                                                   'last_id_fetched_type')
        find_filter['$gte'] = common.string_to_class(last_id_fetched, last_id_fetched_type)

    query_message = 'Querying {} with:\n\tFind Parameters: {}'.format(
        stream['tap_stream_id'],
        find_filter)
    if projection:
        query_message += '\n\tProjection: {}'.format(projection)
    # pylint: disable=logging-format-interpolation
    LOGGER.info(query_message)

    cond = {'_id': find_filter}
    projection = {"details.availability": False}

    rows_saved = 0
    time_extracted = utils.now()
    start_time = time.time()

    schema = {"type": "object", "properties": {}}
    for row in _find_until_complete(collection, cond, projection, stream['stream'], stream['schema']):
        rows_saved += 1

        schema_build_start_time = time.time()

        if common.row_to_schema(schema, row):
            common.SCHEMA_COUNT[stream['tap_stream_id']] += 1
        common.SCHEMA_TIMES[stream['tap_stream_id']] += time.time() - schema_build_start_time

        record_message = common.row_to_singer_record(stream,
                                                     row,
                                                     stream_version,
                                                     time_extracted)

        singer.write_message(record_message)

        # child entities might not have '_id'
        row_id = row.get('_id', False) or row.get('parent_id', False)
        if "parent_id" in row.keys():
            row_id_type = row.get('parent_id', False) and 'ObjectId'
        else:
            row_id_type = row.get('_id', False) and row.get('_id').__class__.__name__ or 'int'

        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'last_id_fetched',
                                      common.class_to_string(row_id,
                                                             row_id_type))
        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'last_id_fetched_type',
                                      row_id_type)

        if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    common.COUNTS[tap_stream_id] += rows_saved
    common.TIMES[tap_stream_id] += time.time()-start_time

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, stream['tap_stream_id'], 'max_id_value')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'max_id_type')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'last_id_fetched')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'last_id_fetched_type')

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(activate_version_message)

    LOGGER.info('Syncd {} records for {}'.format(rows_saved, tap_stream_id))


def _find_until_complete(collection, cond, projection, stream, schema):
    with collection.find_raw_batches(cond, projection, sort=[("_id", pymongo.ASCENDING)]) as cursor:
        for row in common.iterate_over_raw_batch_cursor(cursor):
            row_id = row.get("_id")

            # child streams
            if collection.name != stream:
                if not row.get(stream, False):
                    continue

                child_row = row[stream]
                if isinstance(child_row, dict):
                    try:
                        child_row['parent_id'] = bson.objectid.ObjectId(row_id)
                    except:
                        child_row['parent_id'] = row_id
                    yield common.recursive_conform_to_schema(schema, child_row)

                elif isinstance(child_row, list):
                    for child in child_row:
                        if isinstance(child, dict):
                            try:
                                child['parent_id'] = bson.objectid.ObjectId(row_id)
                            except:
                                child['parent_id'] = row_id
                            yield common.recursive_conform_to_schema(schema, child)
            else:
                yield common.recursive_conform_to_schema(schema, row)
