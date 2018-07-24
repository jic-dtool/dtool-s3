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
