import os

import pytest

from dtoolcore.filehasher import md5sum_hexdigest

from . import tmp_uuid_and_uri  # NOQA
from . import TEST_SAMPLE_DATA
from . import tmp_directory, tmp_env_var


def _prefix_contains_something(storage_broker, prefix):
    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)
    prefix_objects = list(
        bucket.objects.filter(Prefix=prefix).all()
    )
    return len(prefix_objects) > 0


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

    # At this point the temporary fragments should exist.
    assert _prefix_contains_something(
        proto_dataset._storage_broker,
        proto_dataset._storage_broker.fragments_key_prefix
    )

    proto_dataset.put_readme(content='Hello world!')

    # We shouldn't be able to load this as a DataSet
    with pytest.raises(DtoolCoreTypeError):
        DataSet.from_uri(dest_uri)

    proto_dataset.freeze()

    # Freezing removes the temporary metadata fragments.
    assert not _prefix_contains_something(
        proto_dataset._storage_broker,
        proto_dataset._storage_broker.fragments_key_prefix
    )

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


def test_creation_and_reading(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import ProtoDataSet, generate_admin_metadata

    uuid, dest_uri = tmp_uuid_and_uri

    name = "func_test_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    sample_data_path = os.path.join(TEST_SAMPLE_DATA)

    # Create a proto dataset
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None)
    proto_dataset.create()
    proto_dataset.put_readme("")

    assert proto_dataset.name == "func_test_dataset"

    # Test reading from URI.
    proto_dataset = ProtoDataSet.from_uri(dest_uri)
    assert proto_dataset.name == "func_test_dataset"

    # Test get/put readme.
    assert proto_dataset.get_readme_content() == ""
    proto_dataset.put_readme("Hello world!")
    assert proto_dataset.get_readme_content() == "Hello world!"

    # Test putting a local file
    handle = "tiny.png"
    local_file_path = os.path.join(sample_data_path, 'tiny.png')
    proto_dataset.put_item(local_file_path, handle)
    assert handle in list(proto_dataset._storage_broker.iter_item_handles())

    # Test properties of that file
    expected_hash = md5sum_hexdigest(
        os.path.join(sample_data_path, 'tiny.png')
    )
    item_properties = proto_dataset._storage_broker.item_properties(handle)
    assert item_properties['relpath'] == 'tiny.png'
    assert item_properties['size_in_bytes'] == 276
    assert item_properties['hash'] == expected_hash
    assert 'utc_timestamp' in item_properties

    # Add metadata
    proto_dataset.add_item_metadata(handle, 'foo', 'bar')
    proto_dataset.add_item_metadata(
        handle,
        'key',
        {'subkey': 'subval',
         'morekey': 'moreval'}
    )

    # Test metadata retrieval
    metadata = proto_dataset._storage_broker.get_item_metadata(handle)
    assert metadata == {
        'foo': 'bar',
        'key': {
            'subkey': 'subval',
            'morekey': 'moreval'
        }
    }

    # Add another item and test manifest
    from dtoolcore import __version__
    from dtoolcore.utils import generate_identifier
    local_file_path = os.path.join(sample_data_path, 'real_text_file.txt')
    proto_dataset.put_item(local_file_path, 'real_text_file.txt')
    second_handle = 'real_text_file.txt'
    generated_manifest = proto_dataset.generate_manifest()
    assert generated_manifest['hash_function'] == 'md5sum_hexdigest'
    assert generated_manifest['dtoolcore_version'] == __version__
    expected_identifier = generate_identifier(second_handle)
    assert expected_identifier in generated_manifest['items']
    assert generated_manifest['items'][expected_identifier]['relpath'] \
        == 'real_text_file.txt'
    expected_hash = md5sum_hexdigest(local_file_path)
    assert generated_manifest['items'][expected_identifier]['hash'] \
        == expected_hash


def test_files_with_whitespace(tmp_uuid_and_uri):  # NOQA

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
    proto_dataset.put_item(local_file_path, 'tiny with space.png')
    proto_dataset.freeze()

    # Read in a dataset
    dataset = DataSet.from_uri(dest_uri)

    expected_identifier = generate_identifier('tiny with space.png')
    assert expected_identifier in dataset.identifiers
    assert len(dataset.identifiers) == 1


def test_item_local_abspath_with_clean_cache(tmp_uuid_and_uri):  # NOQA

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet
    from dtoolcore.utils import generate_identifier

    uuid, dest_uri = tmp_uuid_and_uri

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

    identifier = generate_identifier('tiny.png')

    with tmp_directory() as cache_dir:
        with tmp_env_var("DTOOL_S3_CACHE_DIRECTORY", cache_dir):

            dataset = DataSet.from_uri(dest_uri)
            fpath = dataset.item_content_abspath(identifier)

            assert os.path.isfile(fpath)
