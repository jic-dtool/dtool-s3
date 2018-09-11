"""Test comparing local and remote dataset."""

import os

from . import tmp_uuid_and_uri  # NOQA
from . import tmp_directory

from . import TEST_SAMPLE_DATA


def test_copy_and_diff(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    import dtoolcore
    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet
    from dtoolcore.compare import (
        diff_identifiers,
        diff_sizes,
        diff_content,
    )

    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    sample_data_path = os.path.join(TEST_SAMPLE_DATA)
    local_file_path = os.path.join(sample_data_path, 'tiny.png')

    # Create a minimal dataset
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata
    )
    proto_dataset.create()
    proto_dataset.put_readme(content='---\ndescription: test')
    proto_dataset.put_item(local_file_path, 'tiny.png')
    proto_dataset.freeze()

    remote_dataset = DataSet.from_uri(dest_uri)

    with tmp_directory() as local_dir:
        local_uri = dtoolcore.copy(dest_uri, local_dir)
        assert local_uri.startswith("file:/")
        local_dataset = DataSet.from_uri(local_uri)
        assert len(diff_identifiers(local_dataset, remote_dataset)) == 0
        assert len(diff_sizes(local_dataset, remote_dataset)) == 0
        assert len(diff_content(local_dataset, remote_dataset)) == 0
