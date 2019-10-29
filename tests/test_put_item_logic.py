"""Test the more robust put_item logic."""

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def test_get_object():
    from dtool_s3.storagebroker import _get_object  # NOQA


def test_upload_file():
    from dtool_s3.storagebroker import _upload_file  # NOQA


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
