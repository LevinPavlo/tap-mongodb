#!/usr/bin/env python3
import copy
import logging
import time

import bson
import pymongo
import singer
import tap_mongodb.sync_strategies.common as common
from bson import errors
from singer import metadata, utils

LOGGER = singer.get_logger()


def update_bookmark(row, state, tap_stream_id, replication_key_name):
    if row.get("parent_id", False):
        replication_key_name = "parent_id"
    replication_key_value = row.get(replication_key_name)
    if replication_key_value:
        if replication_key_name == "parent_id":
            replication_key_type = "ObjectId"
        else:
            replication_key_type = replication_key_value.__class__.__name__

        replication_key_value_bookmark = common.class_to_string(replication_key_value,
                                                                replication_key_type)
        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'replication_key_value',
                                      replication_key_value_bookmark)
        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'replication_key_type',
                                      replication_key_type)


def wrap_row(schema, row, stream, key_properties, tap_stream_id, schema_build_start_time):
    if common.row_to_schema(schema, row):
        singer.write_message(singer.SchemaMessage(
            stream=common.calculate_destination_stream_name(stream),
            schema=schema,
            # TODO: - child might not have an '_id', user parent_id instead
            key_properties=key_properties))
        common.SCHEMA_COUNT[tap_stream_id] += 1
    common.SCHEMA_TIMES[tap_stream_id] += time.time() - schema_build_start_time

# pylint: disable=too-many-locals, too-many-statements
def sync_collection(client, stream, state, projection):
    tap_stream_id = stream['tap_stream_id']
    LOGGER.info('Starting incremental sync for %s', tap_stream_id)

    stream_metadata = metadata.to_map(stream['metadata']).get(())
    LOGGER.info("Metadata object: '%s'", stream_metadata)
    collection = client[stream_metadata['database-name']][stream_metadata['collection']]

    # before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None

    # pick a new table version if last run wasn't interrupted
    if first_run:
        stream_version = int(time.time() * 1000)
    else:
        stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=common.calculate_destination_stream_name(stream),
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_message(activate_version_message)

    # get replication key, and bookmarked value/type
    stream_state = state.get('bookmarks', {}).get(tap_stream_id, {})

    #
    replication_key_name = stream_metadata.get('replication-key')
    replication_key_value_bookmark = stream_state.get('replication_key_value')

    # TODO: child case - increment with parent_id
    if not replication_key_name:
        # repliacation_key_name = '_id'
        # replication_key_value_bookmark['replication_key_name'] = replication_key_name
        return []

    # write state message
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # create query
    find_filter = {}
    if replication_key_value_bookmark:
        find_filter[replication_key_name] = {}
        find_filter[replication_key_name]['$gte'] = \
            common.string_to_class(replication_key_value_bookmark,
                                   stream_state.get('replication_key_type'))
    # log query
    query_message = 'Querying {} with:\n\tFind Parameters: {}'.format(tap_stream_id, find_filter)
    if projection:
        query_message += '\n\tProjection: {}'.format(projection)
    LOGGER.info(query_message)

    # query collection
    schema = {"type": "object", "properties": {}}
    with collection.find(find_filter,
                         projection,
                         sort=[(replication_key_name, pymongo.ASCENDING)]) as cursor:
        rows_saved = 0
        time_extracted = utils.now()
        start_time = time.time()
        while True:
            try:
                row = next(cursor)
            except StopIteration:
                break
            except errors.InvalidBSON as err:
                logging.warning("ignored invalid record: {}".format(str(err)))
                continue
            schema_build_start_time = time.time()
            row_id = row.get("_id")
            if collection.name != stream['stream']:
                if not row.get(stream['stream'], False):
                    continue
                child_row = row[stream['stream']] 

                if isinstance(child_row, dict):
                    child_row["parent_id"] = bson.objectid.ObjectId(row_id)
                    key_properties=['parent_id']
                    row = common.recursive_conform_to_schema(stream['schema'], child_row)
                    wrap_row(schema, row, stream, key_properties, tap_stream_id, schema_build_start_time)
                elif isinstance(child_row, list):
                    for child in child_row:
                        child["parent_id"] = bson.objectid.ObjectId(row_id)
                        key_properties=['parent_id']
                        row = common.recursive_conform_to_schema(stream['schema'], child)
                        wrap_row(schema, row, stream, key_properties, tap_stream_id, schema_build_start_time)
            else:
                key_properties=['_id']
                wrap_row(schema, row, stream, key_properties, tap_stream_id, schema_build_start_time)
            record_message = common.row_to_singer_record(stream,
                                                         row,
                                                         stream_version,
                                                         time_extracted)

            # gen_schema = common.row_to_schema_message(schema, record_message.record, row)
            # if DeepDiff(schema, gen_schema, ignore_order=True) != {}:
            #   emit gen_schema
            #   schema = gen_schema
            singer.write_message(record_message)
            rows_saved += 1

            update_bookmark(row, state, tap_stream_id, replication_key_name)

            if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


        common.COUNTS[tap_stream_id] += rows_saved
        common.TIMES[tap_stream_id] += time.time()-start_time

    singer.write_message(activate_version_message)

    LOGGER.info('Syncd %s records for %s', rows_saved, tap_stream_id)
