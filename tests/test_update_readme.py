import time

from . import tmp_uuid_and_uri  # NOQA


def test_update_readme(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet

    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    # Create a minimal dataset
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None)
    proto_dataset.create()
    proto_dataset.put_readme("First")
    proto_dataset.put_readme("Hello world")
    proto_dataset.freeze()

    # Read in a dataset
    dataset = DataSet.from_uri(dest_uri)

    assert len(dataset._storage_broker._list_historical_readme_keys()) == 0

    dataset.put_readme("Updated")

    assert len(dataset._storage_broker._list_historical_readme_keys()) == 1

    key = dataset._storage_broker._list_historical_readme_keys()[0]
    content = dataset._storage_broker.get_text(key)
    assert content == 'Hello world'

    time.sleep(0.1)

    dataset.put_readme('Updated again')
    assert dataset.get_readme_content() == 'Updated again'


def test_list_historical_readme_keys_uses_configured_endpoint(tmp_uuid_and_uri, monkeypatch):  # NOQA
    """Listing README backups must use the broker's configured S3 resource.

    Previously a fresh ``boto3.resource('s3')`` was created, which ignores
    custom endpoints and credentials (e.g. MinIO) configured via
    ``DTOOL_S3_ENDPOINT_<bucket>`` and friends.
    """
    import boto3

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet

    uuid, dest_uri = tmp_uuid_and_uri

    admin_metadata = generate_admin_metadata("my_dataset")
    admin_metadata["uuid"] = uuid
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None)
    proto_dataset.create()
    proto_dataset.put_readme("First")
    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)
    dataset.put_readme("Updated")

    def unconfigured_resource(*args, **kwargs):
        raise AssertionError("boto3.resource must not be called directly")

    monkeypatch.setattr(boto3, "resource", unconfigured_resource)

    keys = dataset._storage_broker._list_historical_readme_keys()
    assert len(keys) == 1
