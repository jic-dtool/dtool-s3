
import os

import pytest

from dtoolcore.filehasher import md5sum_hexdigest

from . import tmp_uuid_and_uri  # NOQA
from . import TEST_SAMPLE_DATA


def test_basic_workflow(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet
    from dtoolcore.utils import generate_identifier

    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    sample_data_path = os.path.join(TEST_SAMPLE_DATA)
    local_file_path = os.path.join(sample_data_path, 'tiny.png')

    # Create a minimal dataset
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None)
    proto_dataset.create()
    proto_dataset.put_item(local_file_path, 'tiny.png')
    proto_dataset.freeze()

    # Read in a dataset
    dataset = DataSet.from_uri(dest_uri)

    expected_identifier = generate_identifier('tiny.png')
    assert expected_identifier in dataset.identifiers
    assert len(dataset.identifiers) == 1


def test_proto_dataset_freeze_functional(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import (
        generate_admin_metadata,
        DataSet,
        ProtoDataSet,
        DtoolCoreTypeError
    )
    from dtoolcore.utils import generate_identifier

    name = "func_test_dataset_freeze"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    sample_data_path = os.path.join(TEST_SAMPLE_DATA)

    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None
    )
    proto_dataset.create()

    filenames = ['tiny.png', 'actually_a_png.txt', 'another_file.txt']
    for filename in filenames:
        local_file_path = os.path.join(sample_data_path, filename)
        proto_dataset.put_item(local_file_path, filename)
        proto_dataset.add_item_metadata(
            filename,
            'namelen',
            len(filename)
        )
        proto_dataset.add_item_metadata(
            filename,
            'firstletter',
            filename[0]
        )

    proto_dataset.put_readme(content='Hello world!')

    # We shouldn't be able to load this as a DataSet
    with pytest.raises(DtoolCoreTypeError):
        DataSet.from_uri(dest_uri)

    proto_dataset.freeze()

    # Freezing removes the temporary metadata fragments directory.
    # assert not os.path.isdir(
    #     proto_dataset._storage_broker._metadata_fragments_abspath)

    # Now we shouln't be able to load as a ProtoDataSet
    with pytest.raises(DtoolCoreTypeError):
        ProtoDataSet.from_uri(dest_uri)

    # But we can as a DataSet
    dataset = DataSet.from_uri(dest_uri)
    assert dataset.name == 'func_test_dataset_freeze'

    # Test identifiers
    expected_identifiers = map(generate_identifier, filenames)
    assert set(dataset.identifiers) == set(expected_identifiers)

    # Test readme contents
    assert dataset.get_readme_content() == "Hello world!"

    # Test item
    expected_identifier = generate_identifier('tiny.png')
    expected_hash = md5sum_hexdigest(
        os.path.join(sample_data_path, 'tiny.png')
    )
    item_properties = dataset.item_properties(expected_identifier)
    assert item_properties['relpath'] == 'tiny.png'
    assert item_properties['size_in_bytes'] == 276
    assert item_properties['hash'] == expected_hash

    # Test accessing item
    expected_identifier = generate_identifier('another_file.txt')
    fpath = dataset.item_content_abspath(expected_identifier)

    with open(fpath) as fh:
        contents = fh.read()

    assert contents == "Hello\n"

    # Test overlays have been created properly
    namelen_overlay = dataset.get_overlay('namelen')
    expected_identifier = generate_identifier('another_file.txt')
    assert namelen_overlay[expected_identifier] == len('another_file.txt')