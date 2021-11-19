import copy

from singer import metadata


def get_full_catalog(sample_catalog, rediscovered_catalog):
    """
    Update rediscovered catalog with 'selected' and 'replication-method' info
    """
    rediscovered_streams = [
        rs["stream"] for rs in rediscovered_catalog.get("streams", [])
    ]
    # get matched (selected) streams
    matched_streams = [
        s for s in sample_catalog["streams"] if s["stream"] in rediscovered_streams
    ]
    selected_streams = get_selected_streams(matched_streams)

    for rediscovered_stream in rediscovered_catalog.get("streams", []):
        if rediscovered_stream["stream"] in list(selected_streams.keys()):
            # go over metadatas
            metadata = rediscovered_stream["metadata"]
            for meta in rediscovered_stream["metadata"]:
                index = metadata.index(meta)
                metadata[index]["metadata"]["selected"] = True
                metadata[index]["metadata"][
                    "replication-method"
                ] = selected_streams.get(rediscovered_stream["stream"])[
                    "replication-method"
                ]
                metadata[index]["metadata"]["replication-key"] = selected_streams.get(
                    rediscovered_stream["stream"]
                )["replication-key"]
            rediscovered_stream["metadata"] = metadata
    return rediscovered_catalog, list(selected_streams.keys())


def get_selected_streams(streams):
    selected_streams = {}
    for stream in streams:
        metadata = stream.get("metadata", [])
        replication_method = (
            metadata[0]["metadata"]["replication-method"] or "FULL_TABLE"
        )
        if replication_method != "FULL_TABLE":
            replication_key = metadata[0]["metadata"]["replication-key"]
            selected_streams.update(
                {
                    stream["stream"]: {
                        "replication-method": replication_method,
                        "replication-key": replication_key,
                    }
                }
            )
        else:
            selected_streams.update(
                {stream["stream"]: {"replication-method": replication_method}}
            )
    return selected_streams
