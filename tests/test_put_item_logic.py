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


def test_put_item_with_retry_simulating_ambigious_multipart_upload_error():
    import dtool_s3.storagebroker

    # Mock scenario where upload fails, and retruns ambigious faile
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