# Synchronization
import copy
import json
import singer
from singer import metadata, metrics
import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.incremental as incremental

LOGGER = singer.get_logger()


def do_sync(client, catalog, state, selected_stream=None):
    all_streams = catalog['streams']
    streams_to_sync = get_streams_to_sync(all_streams, state, selected_stream)
    LOGGER.info("State: %s", state)
    LOGGER.info("Streams to syc: %s", streams_to_sync)
    for stream in streams_to_sync:
        sync_stream(client, stream, state)

    LOGGER.info(common.get_sync_summary(catalog))


def is_stream_selected(stream):
    mdata = metadata.to_map(stream['metadata'])
    is_selected = metadata.get(mdata, (), 'selected')

    # pylint: disable=singleton-comparison
    return is_selected == True


def get_streams_to_sync(streams, state, selected_stream=None):
    # get selected streams
    selected_streams = [s for s in streams if is_stream_selected(s)]

    # prioritize streams that have not been processed
    streams_with_state = []
    streams_without_state = []
    for stream in selected_streams:
        if selected_stream and not stream.get("stream", False) in selected_stream:
            continue

        if state.get('bookmarks', {}).get(stream['tap_stream_id']):
            streams_with_state.append(stream)
        else:
            streams_without_state.append(stream)

    ordered_streams = streams_without_state + streams_with_state

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing,
            ordered_streams))
        non_currently_syncing_streams = list(filter(
            lambda s: s['tap_stream_id'] != currently_syncing, ordered_streams))
        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        streams_to_sync = ordered_streams

    return streams_to_sync


def load_stream_projection(stream):
    md_map = metadata.to_map(stream['metadata'])
    stream_projection = metadata.get(md_map, (), 'tap-mongodb.projection')
    if stream_projection == '' or stream_projection == '""' or not stream_projection:
        return None

    try:
        stream_projection = json.loads(stream_projection)
    except Exception as e:
        err_msg = "The projection: {} for stream {} is not valid json"
        raise common.InvalidProjectionException(err_msg.format(stream_projection,
                                                               stream['tap_stream_id']))

    if stream_projection and stream_projection.get('_id') == 0:
        raise common.InvalidProjectionException(
            "Projection blacklists key property id for collection {}".format(stream['tap_stream_id']))

    return stream_projection


def write_schema_message(stream):
    singer.write_message(singer.SchemaMessage(
        stream=common.calculate_destination_stream_name(stream),
        schema=stream['schema'],
        key_properties=['_id']))


def clear_state_on_replication_change(stream, state):
    md_map = metadata.to_map(stream['metadata'])
    tap_stream_id = stream['tap_stream_id']

    # replication method changed
    current_replication_method = metadata.get(md_map, (), 'replication-method')
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (current_replication_method != last_replication_method):
        log_msg = 'Replication method changed from %s to %s, will re-replicate entire collection %s'
        LOGGER.info(log_msg, last_replication_method, current_replication_method, tap_stream_id)
        state = singer.reset_stream(state, tap_stream_id)

    # replication key changed
    if current_replication_method == 'INCREMENTAL':
        last_replication_key = singer.get_bookmark(state, tap_stream_id, 'replication_key_name')
        current_replication_key = metadata.get(md_map, (), 'replication-key')
        if last_replication_key is not None and (current_replication_key != last_replication_key):
            log_msg = 'Replication Key changed from %s to %s, will re-replicate entire collection %s'
            LOGGER.info(log_msg, last_replication_key, current_replication_key, tap_stream_id)
            state = singer.reset_stream(state, tap_stream_id)
        state = singer.write_bookmark(state, tap_stream_id, 'replication_key_name', current_replication_key)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', current_replication_method)

    return state


def sync_stream(client, stream, state):
    tap_stream_id = stream['tap_stream_id']

    common.COUNTS[tap_stream_id] = 0
    common.TIMES[tap_stream_id] = 0
    common.SCHEMA_COUNT[tap_stream_id] = 0
    common.SCHEMA_TIMES[tap_stream_id] = 0

    md_map = metadata.to_map(stream['metadata'])
    replication_method = metadata.get(md_map, (), 'replication-method')
    database_name = metadata.get(md_map, (), 'database-name')

    stream_projection = load_stream_projection(stream)

    # Emit a state message to indicate that we've started this stream
    state = clear_state_on_replication_change(stream, state)
    state = singer.set_currently_syncing(state, stream['tap_stream_id'])
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    write_schema_message(stream)
    common.SCHEMA_COUNT[tap_stream_id] += 1

    with metrics.job_timer('sync_table') as timer:
        timer.tags['database'] = database_name
        timer.tags['table'] = stream['table_name']

        if replication_method == 'LOG_BASED':
            if oplog.oplog_has_aged_out(client, state, tap_stream_id):
                # remove all state for stream
                # then it will do a full sync and start oplog again.
                LOGGER.info("Clearing state because Oplog has aged out")
                state.get('bookmarks', {}).pop(tap_stream_id)

            # make sure initial full table sync has been completed
            if not singer.get_bookmark(state, tap_stream_id, 'initial_full_table_complete'):
                msg = 'Must complete full table sync before starting oplog replication for %s'
                LOGGER.info(msg, tap_stream_id)

                # only mark current ts in oplog on first sync so tap has a
                # starting point after the full table sync
                if singer.get_bookmark(state, tap_stream_id, 'version') is None:
                    collection_oplog_ts = oplog.get_latest_ts(client)
                    oplog.update_bookmarks(state, tap_stream_id, collection_oplog_ts)

                full_table.sync_collection(client, stream, state, stream_projection)

            oplog.sync_collection(client, stream, state, stream_projection)

        elif replication_method == 'FULL_TABLE':
            full_table.sync_collection(client, stream, state, stream_projection)

        elif replication_method == 'INCREMENTAL':
            incremental.sync_collection(client, stream, state, stream_projection)
        else:
            raise Exception(
                "only FULL_TABLE, LOG_BASED, and INCREMENTAL replication \
                methods are supported (you passed {})".format(replication_method))

    state = singer.set_currently_syncing(state, None)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
