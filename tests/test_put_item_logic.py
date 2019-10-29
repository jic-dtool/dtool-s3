"""Test the more robust put_item logic."""

import os

from . import tmp_dir_fixture  # NOQA

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def test_get_object():
    from dtool_s3.storagebroker import _get_object  # NOQA


def test_upload_file_simulating_successful_upload():
    from dtool_s3.storagebroker import _upload_file  # NOQA

    # Mock scenario where upload succeeds without need for retry.
    s3client = MagicMock()
    s3client.upload_file = MagicMock(return_value=True)

    value = _upload_file(
            s3client,
            "dummy_fpath",
            "dummy_bucket",
            "dummy_dest_path",
            "dummy_extra_args"
    )

    assert value is True


def test_upload_file_simulating_nosuchupload_failure(tmp_dir_fixture):  # NOQA
    from dtool_s3.storagebroker import _upload_file  # NOQA
    import boto3
    from botocore.stub import Stubber

    s3client = boto3.client('s3')

    fpath = os.path.join(tmp_dir_fixture, "dummy.txt")
    with open(fpath, "w") as fh:
        fh.write("hello")

    with Stubber(s3client) as stubber:
        stubber.add_client_error(
            method='upload_file',
            service_error_code='NoSuchUpload',
            http_status_code=404
        )
        value = _upload_file(
            s3client,
            fpath,
            "dummy_bucket",
            "dummy_dest_path",
            extra_args={}
        )

    assert value is False


def test_put_item_with_retry():
    from dtool_s3.storagebroker import _put_item_with_retry  # NOQA


def test_put_item_with_retry_immediate_success():
    import dtool_s3.storagebroker

    # Mock scenario where upload succeeds without need for retry.
    dtool_s3.storagebroker._upload_file = MagicMock(return_value=True)
    dtool_s3.storagebroker._get_object = MagicMock()

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {},
        4
    )
    dtool_s3.storagebroker._upload_file.assert_called()
    dtool_s3.storagebroker._get_object.assert_not_called()


def test_put_item_with_retry_simulating_upload_error_item_uploaded():
    import dtool_s3.storagebroker

    # Mock scenario where upload fails, and retruns ambigious failure
    # MultipartUploadError, but item has been created in the bucket.
    dtool_s3.storagebroker._upload_file = MagicMock(return_value=False)
    dtool_s3.storagebroker._get_object = MagicMock(return_value=True)

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {},
        4
    )

    dtool_s3.storagebroker._upload_file.assert_called_once()
    dtool_s3.storagebroker._get_object.assert_called_once()


def test_put_item_with_retry_simulating_upload_error_item_doesnt_exist():
    import dtool_s3.storagebroker

    retry_count = 4

    # Mock scenario where upload fails, and retruns ambigious failure
    # MultipartUploadError, but item has been created in the bucket.
    dtool_s3.storagebroker._upload_file = MagicMock(return_value=False)
    dtool_s3.storagebroker._get_object = MagicMock(return_value=None)

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {},
        retry_count
    )

    assert dtool_s3.storagebroker._upload_file.call_count == retry_count + 1
    assert dtool_s3.storagebroker._get_object.call_count == retry_count + 1
