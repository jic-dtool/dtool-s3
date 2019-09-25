import datetime
import logging

import click
import dtoolcore


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class ItemData(object):

    def __init__(self, dataset):
        logging.info("Initilalising ItemData instance")
        self._dataset = dataset

        self.identifier = None
        self.relpath = None
        self.size_in_bytes = None
        self.md5_start = None
        self.md5_end = None
        self.upload_start = None
        self.upload_end = None
        self.num_retries = 0

    def set_identifier(self, i):
        logging.info("Setting identifier: {}".format(i))
        self.identifier = i

    def set_relpath(self, handle):
        logging.info("Setting relpath: {}".format(handle))
        self.relpath = handle

    def set_size_in_bytes(self, i):
        props = self._dataset.item_properties(i)
        logging.info("Setting size_in_bytes: {}".format(
            props["size_in_bytes"]))
        self.size_in_bytes = props["size_in_bytes"]

    def _time_in_minutes(self, start, end):
        diff = end - start
        minutes = diff.total_seconds() / 60
        return minutes

    def set_md5_start(self, start):
        logging.info("Setting md5_start: {}".format(start))
        self.md5_start = start

    def set_md5_end(self, end):
        logging.info("Setting md5_end: {}".format(end))
        self.md5_end = end

    def set_upload_start(self, start):
        logging.info("Setting upload_start: {}".format(start))
        self.upload_start = start

    def set_upload_end(self, end):
        logging.info("Setting upload_end: {}".format(end))
        self.upload_end = end

    def increment_num_retries(self):
        logging.info("Incrementing num_retries")
        self.num_retries += 1

    def reset_all(self):
        logging.info("Resetting all")
        self.identifier = None
        self.md5_start = None
        self.md5_end = None
        self.upload_start = None
        self.upload_end = None
        self.size_in_bytes = None
        self.num_retries = 0

    def echo_csv_header(self):
        header = ",".join([
            "identifier",
            "relpath",
            "size_in_bytes",
            "md5_minutes",
            "upload_minutes",
            "num_retries",
        ]
        click.echo(header)

    def echo_csv(self):
        md5_minutes = self._time_in_minutes(self.md5_start, self.md5_end)
        upload_minutes = self._time_in_minutes(self.upload_start, self.upload_end)  # NOQA
        d = [str(i) for i in[
            self.identifier,
            self.relpath,
            self.size_in_bytes,
            md5_minutes,
            upload_minutes,
            self.num_retries,
        ]]
        csv = ",".join(d)
        logging.info("Echo CSV: {}".format(csv))
        click.echo(csv)


def get_datetime_obj(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f")


def parse_logs(log_file, dataset):

    with open(log_file, "r") as fh:
        item_data = ItemData(dataset)
        item_data.echo_csv_header()

        use_next_line_as_upload_start = False

        for line in fh:
            try:
                date, module, level, msg = line.strip().split(" - ")
                if module == "dtoolcore" and msg.startswith("Get item properties for"):  # NOQA
                    if item_data.upload_start is not None:
                        dt = get_datetime_obj(date)
                        item_data.set_upload_end(dt)
                        item_data.echo_csv()
                        item_data.reset_all()
                    identifier = msg.split()[4]
                    item_data.set_identifier(identifier)
                    item_data.set_size_in_bytes(identifier)
                if module == "botocore.retryhandler" and msg.startswith("Retry needed"):  # NOQA
                    item_data.increment_num_retries()
                if module == "dtoolcore" and msg.startswith("Put item with handle"):  # NOQA
                    handle = msg.split()[4]
                    item_data.set_relpath(handle)
                if module == "dtool_s3.storagebroker" and msg.startswith("Put item"):  # NOQA
                    dt = get_datetime_obj(date)
                    item_data.set_md5_start(dt)
                    use_next_line_as_upload_start = True
                    continue
                if use_next_line_as_upload_start:
                    dt = get_datetime_obj(date)
                    item_data.set_md5_end(dt)
                    item_data.set_upload_start(dt)
                    use_next_line_as_upload_start = False
            except ValueError:
                # message with new line in it!
                pass

        # Deal with last entry.
        dt = get_datetime_obj(date)
        item_data.set_upload_end(dt)
        item_data.echo_csv()


@click.command()
@click.argument("log_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("dataset_uri")
def main(log_file, dataset_uri):

    ds = dtoolcore.DataSet.from_uri(dataset_uri)
    parse_logs(log_file, ds)


if __name__ == "__main__":
    main()
