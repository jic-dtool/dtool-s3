"""Test the S3StorageBroker self description metadata."""

from . import tmp_uuid_and_uri  # NOQA
from . import (
    _key_exists_in_storage_broker,
    _get_data_structure_from_key,
    _get_unicode_from_key
)


def test_writing_of_dtool_structure_file(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtool_s3 import __version__

    # Create a proto dataset.
    uuid, dest_uri = tmp_uuid_and_uri
    name = "test_dtool_structure_file"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None
    )
    proto_dataset.create()

    # Check that the ".dtool/structure.json" file exists.
    expected_s3_key = uuid + '/structure.json'
    assert _key_exists_in_storage_broker(
        proto_dataset._storage_broker,
        expected_s3_key
    )

    expected_content = {
        "dataset_registration_key": "dtool-{}".format(uuid),
        "data_key_infix": "data",
        "fragment_key_infix": "fragments",
        "overlays_key_infix": "overlays",
        "annotations_key_infix": "annotations",
        "tags_key_infix": "tags",
        "structure_key_suffix": "structure.json",
        "dtool_readme_key_suffix": "README.txt",
        "dataset_readme_key_suffix": "README.yml",
        "manifest_key_suffix": "manifest.json",
        "admin_metadata_key_suffix": "dtool",
        "http_manifest_key": "http_manifest.json",
        "storage_broker_version": __version__,
    }
    actual_content = _get_data_structure_from_key(
        proto_dataset._storage_broker,
        expected_s3_key
    )
    assert expected_content == actual_content


def test_writing_of_dtool_readme_file(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import ProtoDataSet, generate_admin_metadata

    # Create a proto dataset.
    uuid, dest_uri = tmp_uuid_and_uri
    name = "test_dtool_readme_file"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None
    )
    proto_dataset.create()

    # Check that the ".dtool/README.txt" file exists.
    expected_s3_key = uuid + '/README.txt'
    assert _key_exists_in_storage_broker(
        proto_dataset._storage_broker,
        expected_s3_key
    )

    actual_content = _get_unicode_from_key(
        proto_dataset._storage_broker,
        expected_s3_key
    )
    assert actual_content.startswith("README")
