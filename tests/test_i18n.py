"""Test use of unicode characters in file names functionality."""

import os

from . import tmp_uuid_and_uri  # NOQA
from . import TEST_SAMPLE_DATA


def test_unicode_to_byte_conversions():

    from dtool_s3.storagebroker import _unicode_to_base64, _base64_to_unicode

    delta = "\u0394"
    base64_str = _unicode_to_base64(delta)
    assert isinstance(base64_str, str)
    assert base64_str == "zpQ="

    back_again = _base64_to_unicode(base64_str)
    assert isinstance(back_again, str)
    assert back_again == delta


def test_handles_with_unicode(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet
    from dtoolcore.utils import generate_identifier

    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    sample_data_path = os.path.join(TEST_SAMPLE_DATA)
    local_file_path = os.path.join(sample_data_path, 'tiny.png')

    handle = "j\u00E4tte_liten.png"
    item_id = generate_identifier(handle)

    # Create a minimal dataset
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None)
    proto_dataset.create()
    proto_dataset.put_item(local_file_path, handle)
    proto_dataset.freeze()

    # Read in a dataset
    dataset = DataSet.from_uri(dest_uri)

    # Check that the relpath property is correct.
    props = dataset.item_properties(item_id)
    assert props["relpath"] == handle
