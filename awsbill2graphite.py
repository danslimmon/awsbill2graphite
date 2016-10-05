#!/usr/bin/env python
import csv
import gzip
import json
import logging
import os
import re
import shutil
import socket
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from operator import attrgetter

import boto3

REGION_NAMES = {
    "US East (N. Virginia)": "us-east-1",
    "US West (N. California)": "us-west-1",
    "US West (Oregon)": "us-west-2",
    "EU (Ireland)": "eu-west-1",
    "EU (Frankfurt)": "eu-central-1",
    "Asia Pacific (Tokyo)": "ap-northeast-1",
    "Asia Pacific (Seoul)": "ap-northeast-2",
    "Asia Pacific (Singapore)": "ap-southeast-1",
    "Asia Pacific (Sydney)": "ap-southeast-2",
    "South America (Sao Paulo)": "sa-east-1",
}

EBS_TYPES = {
    "Magnetic": "standard",
    "General Purpose": "gp2",
    "Provisioned IOPS": "io1",
    "Unknown Storage": "unknown"
}

# As of 2016-09-01, the hourly billing report doesn't have data in the
# 'product/volumeType' column for RDS storage anymore. We have to check
# for a substring of 'lineItem/LineItemDescription' instead.
RDS_STORAGE_TYPES = {
    "Provisioned IOPS Storage": "io1",
    "provisioned GP2 storage": "gp2",
}


def parse_datetime(timestamp):
    """Parses a timestamp in the format 2006-01-02T15:04:05Z."""
    # This way is about 31x faster than arrow.get()
    # and 6.5x faster than datetime.strptime()
    year = int(timestamp[0:4])
    month = int(timestamp[5:7])
    day = int(timestamp[8:10])
    hour = int(timestamp[11:13])
    minute = int(timestamp[14:16])
    second = int(timestamp[17:19])
    return datetime(year, month, day, hour, minute, second)


def open_csv(tempdir, region_name):
    """Opens the latest hourly billing CSV file. Returns an open file object.
       Depending on the AWSBILL_REPORT_PATH environment variable,
       this may involve
       downloading from S3, or it may just open a local file."""
    report_path = os.getenv("AWSBILL_REPORT_PATH")
    if report_path.startswith("file://"):
        csv_path = report_path[len("file://"):]
    elif report_path.startswith("s3://"):
        csv_path = download_latest_from_s3(report_path, tempdir, region_name)
    else:
        raise ValueError("AWSBILL_REPORT_PATH environment variable must start with 'file://' or 's3://'")  # noqa
    return open(csv_path)


def open_output():
    """Opens the file-like object that will be used for output, and returns it.
       Depending on the AWSBILL_GRAPHITE_HOST environment variable,
       writes to this object may be sent to a Graphite
       server or they may be written to stdout."""
    output_host = os.getenv("AWSBILL_GRAPHITE_HOST")
    if output_host is None:
        raise ValueError("AWSBILL_GRAPHITE_HOST environment variable must specify the output destination; you may use 'stdout' to print metrics to stdout")  # noqa
    elif output_host == "stdout":
        output_file = sys.stdout
    else:
        output_port = 2003
        if ":" in output_host:
            output_port = int(output_host.split(":", 1)[1])
            output_host = output_host.split(":", 1)[0]
        output_file = SocketWriter(output_host, output_port)
    return output_file


def s3_primary_manifests(objects):
    """Returns the S3 object(s) corresponding to the relevant primary manifests

       The relevant ones are considered to be the second-most- and most recent
       ones, and they are returned in that order. If there are no billing
       cycles older than the most recent, we return a single-element list with
       only the most recent manifest.

       `objects` should be an iterable of S3 objects."""
    # The path to the billing report manifest is like this:
    #
    # <bucket>/<configured prefix>/hourly_billing/<YYYYmmdd>-<YYYYmmdd>/hourly_billing-Manifest.json  # noqa
    #
    # We look for the most recent timestamp directory and use the manifest
    #  therein to find the most recent billing CSV.
    manifests = [o for o in objects if o.key.endswith("Manifest.json")]

    # Filter to those from the second-most- and most recent billing cycle
    manifests.sort(key=attrgetter("key"), reverse=True)
    cycles = set([])
    for m in manifests:
        rslt = re.search("/(\d{8}-\d{8})/", m.key)
        if rslt is not None:
            cycles.add(rslt.group(1))
    if len(cycles) == 0:
        raise Exception("Failed to find any appropriately-named billing CSVs")
    last_two_cycles = sorted(list(cycles))[-2:]
    if len(last_two_cycles) < 2:
        last_two_cycles = 2 * last_two_cycles
    manifests = [m for m in manifests if
                 last_two_cycles[0] in m.key or last_two_cycles[1] in m.key]

    # The primary manifest(s) will be the one(s) with the shortest path length
    manifests.sort(key=lambda a: len(a.key))
    if last_two_cycles[0] == last_two_cycles[1]:
        # There was only one billing cycle present among the manifests
        return [manifests[0]]
    return [manifests[1], manifests[0]]


def download_latest_from_s3(s3_path, tempdir, region_name):
    """Puts the latest hourly billing report from the given S3 path in a local
       file.

       Returns the path to that file."""
    s3 = boto3.resource("s3", region_name=region_name)
    bucket = s3.Bucket(s3_path.split("/")[2])
    primaries = s3_primary_manifests(bucket.objects.all())
    logging.info("Using primary manifest(s) {0}".format(
        [p.key for p in primaries]
        )
    )

    # Now we parse the manifest to get the path to the latest billing CSV
    s3_csvs = []
    for pri in primaries:
        manifest = json.loads(pri.get()['Body'].read())
        s3_csvs.extend(manifest["reportKeys"])

    # Download each billing CSV to a temp directory and decompress
    try:
        cat_csv_path = os.path.join(tempdir, "billing_full.csv")
        cat_csv = open(cat_csv_path, "w")
        header_written = False
        for s3_csv in s3_csvs:
            logging.info("Downloading CSV from S3: {0}".format(s3_csv))
            local_path = os.path.join(tempdir, s3_csv.split("/")[-1])
            local_file = open(local_path, "w")
            obj = [o for o in bucket.objects.filter(Prefix=s3_csv)][0]
            local_file.write(obj.get()['Body'].read())
            local_file.close()
            logging.info("Decompressing CSV: {0}".format(s3_csv))

            with gzip.open(local_path, "r") as f:
                for line in f:
                    if line.startswith(
                            "identity/LineItemId,"
                    ) and header_written:
                        continue
                    cat_csv.write(line)
                    header_written = True
            # Remove these files as we finish with them to save on disk space
            os.unlink(local_path)
    except Exception, e:
        logging.error(
            "Exception: cleaning up by removing temp directory '{0}'".format(
                tempdir
            )
        )
        shutil.rmtree(tempdir)
        raise e

    cat_csv.close()
    return cat_csv_path


class SocketWriter(object):
    """Wraps a socket object with a file-like write() method."""
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = None

    def write(self, data):
        if self._sock is None:
            logging.info("Connecting to Graphite server at {0}:{1}".format(
                self.host,
                self.port
                )
            )
            self._sock = socket.create_connection((self.host, self.port))
        return self._sock.send(data)


class MetricLedger(object):
    """Processes Row instances and generates timeseries data from them."""
    def __init__(self, timeseries_patterns):
        """Initializes the MetricLedger with alist of TimeseriesPattern
        objects."""
        self._patterns = timeseries_patterns
        self._timeseries = defaultdict(lambda: defaultdict(float))

    def process(self, row):
        """Adds the data from the given Row object to any appropriate
        timeseries."""
        # Skip entries of the wrong type
        if row.content["lineItem/LineItemType"] != "Usage":
            return

        # Skip non-hourly entries
        if row.interval() != 3600:
            return
        for pat in self._patterns:
            if pat.match(row):
                for metric in pat.metric_names(row):
                    self._timeseries[metric][row.end_time()] += row.amount()

    def output(self, output_file):
        formatter = MetricFormatter()
        logging.info("Writing metrics to timeseries database")
        for ts_id, ts in self._timeseries.iteritems():
            for timestamp, value in ts.iteritems():
                output_file.write(formatter.format(ts_id, timestamp, value))
        logging.info("Finished writing %d metrics to timeseries database", len(self._timeseries))

    def get_timeseries(self):
        """Returns self._timeseries (for tests)."""
        return self._timeseries


class MetricFormatter(object):
    """Converts CSV data to Graphite format."""
    def __init__(self):
        self._initial_pieces = []
        if os.getenv("AWSBILL_METRIC_PREFIX") != "":
            self._initial_pieces = [os.getenv("AWSBILL_METRIC_PREFIX")]
        else:
            self._initial_pieces = ["awsbill"]

    def format(self, ts_id, timestamp, value):
        """Returns the Graphite line that corresponds to the given timeseries
        ID, timestamp, and value."""
        pieces = [p for p in self._initial_pieces]
        pieces.append(ts_id)
        metric_name = ".".join(pieces)
        return "{0} {1:04f} {2}\n".format(
            metric_name,
            value,
            timestamp.strftime('%s')
        )


class TimeseriesPattern(object):
    """Describes a set of time series to be generated from the billing data.

       This is an abstract class. Provide an implementation of the match() and
       metric_name() methods."""
    def match(self, row):
        """Determines whether the given Row instance matches the timeseries
        pattern.

           Returns True if so."""
        raise NotImplementedError("This is an abstract class")

    def metric_names(self, row):
        """Returns the names of the metrics to which the given row's amount()
        value should be added.

        We assume that match() has been called on the row already, and
        returned True."""
        raise NotImplementedError("This is an abstract class")


class TsInstanceType(TimeseriesPattern):
    """Describes per-EC2-instance-type Graphite metrics."""
    def match(self, row):
        if row.usage_type():
            return (row.usage_type().startswith("ec2-instance."))
        else:
            pass

    def metric_names(self, row):
        return [".".join((row.region(), row.usage_type()))]


class TsEbsStorage(TimeseriesPattern):
    """Describes per-volume-type EBS storage metric."""
    def match(self, row):
        return row.usage_type().startswith("ebs.storage.")

    def metric_names(self, row):
        return [".".join((row.region(), row.usage_type()))]


class TsEbsPiops(TimeseriesPattern):
    """Describes the metric for PIOPS-month costs."""
    def match(self, row):
        return row.usage_type() == "ebs.piops"

    def metric_names(self, row):
        return [".".join((row.region(), "ebs.piops"))]


class TsEbsIops(TimeseriesPattern):
    """Describes the metric for IOPS costs."""
    def match(self, row):
        return row.usage_type() == "ebs.iops"

    def metric_names(self, row):
        return [".".join((row.region(), "ebs.iops"))]


class TsEbsSnapshot(TimeseriesPattern):
    """Describes the metric for EBS snapshot costs."""
    def match(self, row):
        return row.usage_type() == "ebs.snapshot"

    def metric_names(self, row):
        return [".".join((row.region(), "ebs.snapshot"))]


class TsRdsInstanceType(TimeseriesPattern):
    """Describes per-RDS-instance-type Graphite metrics."""
    def match(self, row):
        return (row.usage_type().startswith("rds-instance."))

    def metric_names(self, row):
        return [".".join((row.region(), row.usage_type()))]


class TsRdsStorage(TimeseriesPattern):
    """Describes per-volume-type RDS storage metric."""
    def match(self, row):
        return row.usage_type().startswith("rds.storage.")

    def metric_names(self, row):
        return [".".join((row.region(), row.usage_type()))]


class TsRdsPiops(TimeseriesPattern):
    """Describes the metric for RDS PIOPS-month costs."""
    def match(self, row):
        return row.usage_type() == "rds.piops"

    def metric_names(self, row):
        return [".".join((row.region(), "rds.piops"))]


class TsElasticacheInstanceType(TimeseriesPattern):
    """Describes per-ElastiCache-instance-type Graphite metrics."""
    def match(self, row):
        return (row.usage_type().startswith("elasticache-instance."))

    def metric_names(self, row):
        return [".".join((row.region(), row.usage_type()))]


class TsRegionTotal(TimeseriesPattern):
    """Describes a Graphite metric containing the sum of all hourly costs per
    region.

       This includes costs that we don't explicitly recognize and break out
       into individual metrics. Any cost that shows up in the billing report
       will go into this metric."""
    def match(self, row):
        return True

    def metric_names(self, row):
        return ["total-cost.{0}".format(row.region())]


class Row(object):
    __slots__ = ["content", "_usage_type"]

    def __init__(self, col_names, row_list):
        """Initializes a Row object, given the names of the CSV columns and
        their values."""
        self.content = dict(zip(col_names, row_list))
        self._usage_type = None

    def region(self):
        """Returns the normalized AWS region for the row, or 'noregion'.

           Normalized region names are like 'us-east-2', 'ap-northeast-1'."""
        if self.content["product/location"] in REGION_NAMES:
            # Most services have product/location set
            return REGION_NAMES[self.content["product/location"]]
        elif self.content["lineItem/AvailabilityZone"] and \
                self.content["lineItem/AvailabilityZone"][-1] in "1234567890":
            # Some services, e.g. ElastiCache, use lineItem/AvailabilityZone
            # instead
            return self.content["lineItem/AvailabilityZone"]
        return "noregion"

    def interval(self):
        """Returns the length of the time interval to which this row
        correpsonds, in seconds."""
        start, end = [parse_datetime(x) for x in
                      self.content["identity/TimeInterval"].split("/", 1)]
        return int((end - start).total_seconds())

    def usage_type(self):
        """Parses the "lineItem/UsageType" field to get at the "subtype"
        (my term).

           Usage types can be of many forms. Here are some examples:

               USE1-USW2-AWS-In-Bytes
               Requests-RBP
               Request
               APN1-DataProcessing-Bytes
               APN1-BoxUsage:c3.2xlarge

           It's a goddamn nightmare. We try our best. Then we return the name
           of the subtype, in the format in which it'll appear in the Graphite
           metric.
           Examples of usage types are:

               ec2-instance.c3-2xlarge
               ebs.storage.io1
               ebs.piops
               rds-instance.db-r3-large

           This method returns the empty string if the usage type isn't
           known."""
        if self._usage_type is not None:
            return self._usage_type
        splut = self.content["lineItem/UsageType"].split("-", 1)
        if len(splut[0]) == 4 and splut[0][0:2] in (
                "US",
                "EU",
                "AP",
                "SA"
        ) and splut[0].isupper() and splut[0][3].isdigit():
            # Stuff before dash was probably a region code like "APN1"
            csv_usage_type = splut[1]
        else:
            csv_usage_type = splut[0]
        self._usage_type = ""

        # EC2
        if csv_usage_type.startswith("BoxUsage:"):
            self._usage_type = self._usage_type_ec2_instance()
        if csv_usage_type == "EBS:VolumeP-IOPS.piops":
            self._usage_type = "ebs.piops"
        if csv_usage_type.startswith("EBS:VolumeUsage"):
            self._usage_type = self._usage_type_ebs_storage()
        if csv_usage_type == "EBS:VolumeIOUsage":
            self._usage_type = "ebs.iops"
        if csv_usage_type == "EBS:SnapshotUsage":
            self._usage_type = "ebs.snapshot"

        # RDS
        if csv_usage_type.startswith("InstanceUsage:") or \
                csv_usage_type.startswith("Multi-AZUsage:"):
            self._usage_type = self._usage_type_rds_instance()
        if csv_usage_type == "RDS:PIOPS" or \
                csv_usage_type == "RDS:Multi-AZ-PIOPS":
            self._usage_type = "rds.piops"
        if csv_usage_type.startswith("RDS:") and \
                csv_usage_type.endswith("Storage"):
            self._usage_type = self._usage_type_rds_storage()

        # ElastiCache
        if csv_usage_type.startswith("NodeUsage:"):
            self._usage_type = self._usage_type_elasticache_instance()

        return self._usage_type

    def _usage_type_ec2_instance(self):
        splut = self.content["lineItem/UsageType"].split(":", 1)
        if len(splut) < 2:
            return None
        instance_type = splut[1].replace(".", "-")
        return "ec2-instance.{0}".format(instance_type)

    def _usage_type_ebs_storage(self):
        if "product/volumeType" in self.content:
            return "ebs.storage.{0}".format(
                EBS_TYPES[self.content["product/volumeType"]]
            )
        else:
            return "ebs.storage.unknown"

    def _usage_type_rds_instance(self):
        splut = self.content["lineItem/UsageType"].split(":", 1)
        if len(splut) < 2:
            return None
        instance_type = splut[1].replace(".", "-")
        return "rds-instance.{0}".format(instance_type)

    def _usage_type_rds_storage(self):
        line_item_description = self.content['lineItem/LineItemDescription']
        volume_type = ""
        for substring in RDS_STORAGE_TYPES.keys():
            if substring in line_item_description:
                volume_type = RDS_STORAGE_TYPES[substring]
        if volume_type == "":
            raise ValueError("Can't determine RDS storage type from line item description: '{0}'".format(line_item_description)) #noqa
        return "rds.storage.{0}".format(volume_type)

    def _usage_type_elasticache_instance(self):
        splut = self.content["lineItem/UsageType"].split(":", 1)
        if len(splut) < 2:
            return None
        instance_type = splut[1].replace(".", "-")
        return "elasticache-instance.{0}".format(instance_type)

    def end_time(self):
        return parse_datetime(
            self.content["identity/TimeInterval"].split("/", 1)[1]
        )

    def tags(self):
        return {}

    def amount(self):
        return float(self.content["lineItem/BlendedCost"])


def new_metric_ledger():
    return MetricLedger([
        # EC2
        TsInstanceType(),
        TsEbsStorage(),
        TsEbsPiops(),
        TsEbsIops(),
        TsEbsSnapshot(),
        # RDS
        TsRdsInstanceType(),
        TsRdsStorage(),
        TsRdsPiops(),
        # ElastiCache
        TsElasticacheInstanceType(),
        # Total
        TsRegionTotal(),
    ])


def generate_metrics(csv_file, output_file):
    """Generates metrics from the given CSV and writes them to the given
    file-like object."""
    reader = csv.reader(csv_file)
    col_names = reader.next()
    # formatter = MetricFormatter()
    ledger = new_metric_ledger()
    logging.info("Calculating billing metrics")
    for row_list in reader:
        row = Row(col_names, row_list)
        ledger.process(row)
    ledger.output(output_file)

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    if os.getenv("REGION_NAME") != '':
        region_name = os.getenv("REGION_NAME")
    else:
        region_name = 'us-west-1'
    try:
        tempdir = tempfile.mkdtemp(".awsbill")
        csv_file = open_csv(tempdir, region_name)
        output_file = open_output()
        generate_metrics(csv_file, output_file)
        logging.info("Removing temp directory '{0}'".format(tempdir))
        shutil.rmtree(tempdir)
        logging.info("Mission complete.")
    except Exception, e:
        logging.exception(e)
