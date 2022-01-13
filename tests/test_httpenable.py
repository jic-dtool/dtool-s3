import os


from . import tmp_uuid_and_uri  # NOQA
from . import TEST_SAMPLE_DATA
from . import tmp_env_var



def test_http_manifest(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet

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
    proto_dataset.put_readme("---\nproject: testing\n")
    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)

    # Test HTTP manifest.
    http_manifest = dataset._storage_broker._generate_http_manifest(expiry=None)  # NOQA
    assert "admin_metadata" in http_manifest
    assert http_manifest["admin_metadata"] == dataset._admin_metadata
    assert "overlays" in http_manifest
    assert "readme_url" in http_manifest
    assert "manifest_url" in http_manifest
    assert "item_urls" in http_manifest
    assert "annotations" in http_manifest
    assert "tags" in http_manifest
    assert set(http_manifest["item_urls"].keys()) == set(dataset.identifiers)


def test_http_enable(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet

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
    proto_dataset.put_readme("---\nproject: testing\n")
    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)

    # Add an annotation.
    dataset.put_annotation("project", "dtool-testing")

    # Add tags.
    dataset.put_tag("amazing")
    dataset.put_tag("stuff")

    access_url = dataset._storage_broker.http_enable()
    assert access_url.find("?") == -1  # This is not a presigned URL dataset.

    assert access_url.startswith("https://")

    dataset_from_http = DataSet.from_uri(access_url)

    # Assert that the annotation has been copied across.
    assert dataset_from_http.get_annotation("project") == "dtool-testing"

    # Asser that the tags are available.
    assert dataset_from_http.list_tags() == ["amazing", "stuff"]

    from dtoolcore.compare import (
        diff_identifiers,
        diff_sizes,
        diff_content
    )

    assert len(diff_identifiers(dataset, dataset_from_http)) == 0
    assert len(diff_sizes(dataset, dataset_from_http)) == 0
    assert len(diff_content(dataset_from_http, dataset)) == 0

    # Make sure that none of the URLs in the manifest are presigned.
    http_manifest = dataset_from_http._storage_broker.http_manifest
    assert http_manifest["manifest_url"].find("?") == -1
    assert http_manifest["readme_url"].find("?") == -1
    for url in http_manifest["item_urls"].values():
        assert url.find("?") == -1
    for url in http_manifest["annotations"].values():
        assert url.find("?") == -1


def test_http_enable_with_presigned_url(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet

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
    proto_dataset.put_readme("---\nproject: testing\n")
    proto_dataset.freeze()

    dataset = DataSet.from_uri(dest_uri)

    # Add an annotation.
    dataset.put_annotation("project", "dtool-testing")

    # Add tags.
    dataset.put_tag("amazing")
    dataset.put_tag("stuff")

    with tmp_env_var("DTOOL_S3_PUBLISH_EXPIRY", "120"):
        access_url = dataset._storage_broker.http_enable()
    assert access_url.find("?") != -1  # This is a presigned URL dataset.

    assert access_url.startswith("https://")

    dataset_from_http = DataSet.from_uri(access_url)

    # Assert that the annotation has been copied across.
    assert dataset_from_http.get_annotation("project") == "dtool-testing"

    # Asser that the tags are available.
    assert dataset_from_http.list_tags() == ["amazing", "stuff"]

    from dtoolcore.compare import (
        diff_identifiers,
        diff_sizes,
        diff_content
    )

    assert len(diff_identifiers(dataset, dataset_from_http)) == 0
    assert len(diff_sizes(dataset, dataset_from_http)) == 0
    assert len(diff_content(dataset_from_http, dataset)) == 0

    # Make sure that all the URLs in the manifest are presigned.
    http_manifest = dataset_from_http._storage_broker.http_manifest
    assert http_manifest["manifest_url"].find("?") != -1
    assert http_manifest["readme_url"].find("?") != -1
    for url in http_manifest["item_urls"].values():
        assert url.find("?") != -1
    for url in http_manifest["annotations"].values():
        assert url.find("?") != -1
