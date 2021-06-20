import sys

import click
import dtoolcore
from dtool_cli.cli import (
    proto_dataset_uri_argument,
    dataset_uri_argument,
)


def _chunks(l, n):  # NOQA
    """Yield successive n-sized chunks from l."""

    for i in range(0, len(l), n):
        yield l[i:i + n]


def _all_objects_in_storage_broker(storage_broker):

    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)

    for obj in bucket.objects.filter(Prefix=storage_broker.uuid).all():
        yield obj.key

    registration_key = "dtool-{}".format(storage_broker.uuid)
    yield registration_key


def _remove_dataset(dataset):

    storage_broker = dataset._storage_broker

    object_key_iterator = _all_objects_in_storage_broker(storage_broker)

    object_key_list = list(object_key_iterator)

    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)
    # Max objects to delete in one API call is 1000, we'll do 500 for safety
    for keys in _chunks(object_key_list, 500):
        keys_as_list_of_dicts = [{'Key': k} for k in keys]
        bucket.objects.delete(
            Delete={'Objects': keys_as_list_of_dicts}
        )


def _confirm_dataset_removal(dataset):
    msg = "Are you sure you want to delete {} from {}?".format(
        dataset.name,
        dataset.base_uri
    )
    click.secho(
        msg,
        fg="red"
    )

    # Make sure the user knows what the dataset is.
    ds_name_confirm = click.prompt(
        'Please confirm dataset name',
        type=str
    )
    if ds_name_confirm != dataset.name:
        click.secho("Cancelling removal of dataset")
        sys.exit()

    # Make sure the user knows what base URI the dataset is in.
    ds_base_uri_confirm = click.prompt(
        'Please confirm the base URI',
        type=str
    )
    if ds_base_uri_confirm != dataset.base_uri:
        click.secho("Cancelling removal of dataset")
        sys.exit()


@click.group()
def remove_dataset():
    """Permanently remove dataset from bucket."""


@remove_dataset.command()
@proto_dataset_uri_argument
def proto(proto_dataset_uri):
    """Permanently remove proto dataset from bucket."""
    ds = dtoolcore.ProtoDataSet.from_uri(proto_dataset_uri)

    # Prompt the user for confirmation.
    _confirm_dataset_removal(ds)

    # Delete the dataset from the bucket!
    _remove_dataset(ds)
    click.secho("Dataset deleted", fg="green")


@remove_dataset.command()
@dataset_uri_argument
def frozen(dataset_uri):
    """Permanently remove frozen dataset from bucket."""
    ds = dtoolcore.DataSet.from_uri(dataset_uri)

    # Prompt the user for confirmation.
    _confirm_dataset_removal(ds)

    # Delete the dataset from the bucket!
    _remove_dataset(ds)
    click.secho("Dataset deleted", fg="green")


if __name__ == "__main__":
    remove_dataset()
