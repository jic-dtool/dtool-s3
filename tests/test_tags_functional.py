"""Test the dataset tag functionality."""

import pytest

from . import tmp_uuid_and_uri  # NOQA


def test_tags_functional(tmp_uuid_and_uri):  # NOQA

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

    # Test put_tag on proto dataset.
    proto_dataset.put_tag("testing")

    proto_dataset.freeze()

    dataset = DataSet.from_uri(proto_dataset.uri)
    assert dataset.list_tags() == ["testing"]

    dataset.put_tag("amazing")
    dataset.put_tag("stuff")
    assert dataset.list_tags() == ["amazing", "stuff", "testing"]

    dataset.delete_tag("stuff")
    assert dataset.list_tags() == ["amazing", "testing"]

    # Putting the same tag is idempotent.
    dataset.put_tag("amazing")
    dataset.put_tag("amazing")
    dataset.put_tag("amazing")
    assert dataset.list_tags() == ["amazing", "testing"]

    # Tags can only be strings.
    from dtoolcore import DtoolCoreValueError
    with pytest.raises(DtoolCoreValueError):
        dataset.put_tag(1)

    # Tags need to adhere to the utils.name_is_valid() rules.
    from dtoolcore import DtoolCoreInvalidNameError
    with pytest.raises(DtoolCoreInvalidNameError):
        dataset.put_tag("!invalid")

    # Deleting a non exiting tag does not raise. It silently succeeds.
    dataset.delete_tag("dontexist")
