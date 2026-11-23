"""Regression tests for overlay, annotation and metadata names with dots.

dtoolcore's ``name_is_valid`` allows dots in names (e.g. "v1.2"). Previously
the S3 storage broker split object keys on dots, so that
``list_overlay_names`` and ``list_annotation_names`` raised ``ValueError`` for
the whole dataset and item metadata keys lost everything before the last dot.
"""

import os

from . import tmp_uuid_and_uri  # NOQA
from . import TEST_SAMPLE_DATA


def _create_proto_dataset(uuid, uri):
    from dtoolcore import ProtoDataSet, generate_admin_metadata

    admin_metadata = generate_admin_metadata("my_dataset")
    admin_metadata["uuid"] = uuid
    proto_dataset = ProtoDataSet(
        uri=uri,
        admin_metadata=admin_metadata,
        config_path=None)
    proto_dataset.create()
    proto_dataset.put_readme("")
    proto_dataset.put_item(
        os.path.join(TEST_SAMPLE_DATA, "tiny.png"), "tiny.png"
    )
    return proto_dataset


def test_annotation_names_with_dots(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import DataSet

    uuid, dest_uri = tmp_uuid_and_uri
    proto_dataset = _create_proto_dataset(uuid, dest_uri)
    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)
    dataset.put_annotation("v1.2", "release")
    dataset.put_annotation("project", "demo")

    assert sorted(dataset.list_annotation_names()) == ["project", "v1.2"]
    assert dataset.get_annotation("v1.2") == "release"


def test_overlay_names_with_dots(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import DataSet

    uuid, dest_uri = tmp_uuid_and_uri
    proto_dataset = _create_proto_dataset(uuid, dest_uri)
    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)
    overlay = {identifier: True for identifier in dataset.identifiers}
    dataset.put_overlay("is.png", overlay)

    assert dataset.list_overlay_names() == ["is.png"]
    assert dataset.get_overlay("is.png") == overlay


def test_item_metadata_key_with_dot(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import DataSet
    from dtoolcore.utils import generate_identifier

    uuid, dest_uri = tmp_uuid_and_uri
    proto_dataset = _create_proto_dataset(uuid, dest_uri)
    proto_dataset.add_item_metadata("tiny.png", "my.key", 42)
    proto_dataset.add_item_metadata("tiny.png", "other", "x")

    assert proto_dataset._storage_broker.get_item_metadata("tiny.png") == {
        "my.key": 42,
        "other": "x",
    }

    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)
    assert sorted(dataset.list_overlay_names()) == ["my.key", "other"]
    assert dataset.get_overlay("my.key") == {
        generate_identifier("tiny.png"): 42
    }
