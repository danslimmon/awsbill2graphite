#!/usr/bin/env python
import csv
import gzip
import json
import logging
import os
import shutil
import socket
import sys
import tempfile
from datetime import timedelta
from collections import defaultdict

import boto3
import arrow

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

LINE_ITEMS = {
    ("AmazonEC2", "OnDemand", "ec2-instance"): True,
}

def open_csv():
    """Opens the latest hourly billing CSV file. Returns an open file object.
    
       Depending on the AWSBILL_REPORT_PATH environment variable, this may involve
       downloading from S3, or it may just open a local file."""
    report_path = os.getenv("AWSBILL_REPORT_PATH")
    if report_path.startswith("file://"):
        csv_path = report_path[len("file://")-1:]
    elif report_path.startswith("s3://"):
        csv_path = download_latest_from_s3(report_path)
    else:
        raise ValueError("AWSBILL_REPORT_PATH environment variable must start with 'file://' or 's3://'")
    return open(csv_path)

def open_output():
    """Opens the file-like object that will be used for output, and returns it.

       Depending on the AWSBILL_GRAPHITE_HOST environment variable, writes to this
       object may be sent to a Graphite server or they may be written to stdout."""
    output_host = os.getenv("AWSBILL_GRAPHITE_HOST")
    if output_host is None:
        raise ValueError("AWSBILL_GRAPHITE_HOST environment variable must specify the output destination; you may use 'stdout' to print metrics to stdout")
    elif output_host == "stdout":
        output_file = sys.stdout
    else:
        output_port = 2003
        if ":" in output_host:
            output_host = output_host.split(":", 1)[0]
            output_port = int(output_host.split(":", 1)[1])
        logging.info("Connecting to Graphite server '{0}' on port {1}".format(
            output_host, output_port))
        output_file = SocketWriter(socket.create_connection((output_host, output_port)))
    return output_file

def download_latest_from_s3(s3_path):
    """Puts the latest hourly billing report from the given S3 path in a local file.

       Returns the path to that file."""
    # The path to the billing report manifest is like this:
    #
    # <bucket>/<configured prefix>/hourly_billing/<YYYYmmdd>-<YYYYmmdd>/hourly_billing-Manifest.json
    #
    # We look for the most recent timestamp directory and use the manifest therein to
    # find the most recent billing CSV.
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_path.split("/")[2])
    manifests = [o for o in bucket.objects.all() if "Manifest.json" in o.key]
    # The primary manifest will be the one with the shortest path length
    manifests.sort(lambda a, b: cmp(len(a.key), len(b.key)))
    primary = manifests[0]

    # Now we parse the manifest to get the path to the latest billing CSV
    manifest = json.loads(primary.get()['Body'].read())
    s3_csvs = manifest["reportKeys"]

    # Download each billing CSV to a temp directory and decompress
    tempdir = tempfile.mkdtemp(".awsbill")
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
                    if line.startswith("identity/LineItemId,") and header_written:
                        continue
                    cat_csv.write(line)
                    header_written = True
                os.unlink(local_path)
    except Exception, e:
        logging.error("Cleaning up by removing temp directory '{0}'".format(tempdir))
        shutil.rmtree(tempdir)
        raise e

    cat_csv.close()
    return cat_csv_path


class SocketWriter(object):
    """Wraps a socket object with a file-like write() method."""
    def __init__(self, sock):
        self.sock = sock
    def write(self, data):
        return self.sock.send(data)


class MetricLedger(object):
    """Processes Row instances and generates timeseries data from them."""
    def __init__(self, timeseries_patterns):
        """Initializes the MetricLedger with a list of TimeseriesPattern objects."""
        self._patterns = timeseries_patterns
        self._timeseries = defaultdict(lambda: defaultdict(float))
    def process(self, row):
        """Adds the data from the given Row object to any appropriate timeseries."""
        for pat in self._patterns:
            if pat.match(row):
                self._timeseries[pat.metric_name(row)][row.end_time()] += row.amount()
    def output(self, output_file):
        formatter = MetricFormatter()
        for ts_id, ts in self._timeseries.iteritems():
            for timestamp, value in ts.iteritems():
                output_file.write(formatter.format(ts_id, timestamp, value))


class MetricFormatter(object):
    """Converts CSV data to Graphite format."""
    def __init__(self):
        self._initial_pieces = []
        if os.getenv("AWSBILL_METRIC_PREFIX") != "":
            self._initial_pieces = [os.getenv("AWSBILL_METRIC_PREFIX")]

    def format(self, ts_id, timestamp, value):
        """Returns the Graphite line that corresponds to the given timeseries ID, timestamp, and value."""
        pieces = [p for p in self._initial_pieces]
        pieces.append(ts_id)
        metric_name = ".".join(pieces)
        return "{0} {1:04f} {2}\n".format(metric_name, value, timestamp.strftime('%s'))


class TimeseriesPattern(object):
    """Describes a set of time series to be generated from the billing data.

       This is an abstract class. Provide an implementation of the match() and
       metric_name() methods."""
    def match(self, row):
        """Determines whether the given Row instance matches the timeseries pattern.

           Returns True if so."""
        raise NotImplementedError("This is an abstract class")
    def metric_name(self, row):
        """Returns the name of the metric to which the given row's amount() value should be added.

           We assume that match() has been called on the row already, and returned
           True."""
        raise NotImplementedError("This is an abstract class")


class ByInstanceType(TimeseriesPattern):
    """Describes per-EC2-instance-type Graphite metrics."""
    def match(self, row):
        return (row.usage_type() == "ec2-instance" and len(row.tags()) == 0)
    def metric_name(self, row):
        return ".".join((row.region(), row.usage_type(), row.instance_type()))


class AllCosts(TimeseriesPattern):
    """Describes a Graphite metric containing the sum of all hourly costs"""
    def match(self, row):
        return True
    def metric_name(self, row):
        return "all-regions.all-types.total-cost"


class Row(object):
    __slots__ = ["content"]
    def __init__(self, col_names, row_list):
        """Initializes a Row object, given the names of the CSV columns and their values."""
        self.content = dict(zip(col_names, row_list))

    def region(self):
        """Returns the normalized AWS region for the row, or 'noregion'.

           Normalized region names are like 'us-east-2', 'ap-northeast-1'."""
        if REGION_NAMES.has_key(self.content["product/location"]):
            return REGION_NAMES[self.content["product/location"]]
        return "noregion"

    def interval(self):
        """Returns the length of the time interval to which this row correpsonds, in seconds."""
        start, end = [arrow.get(x) for x in self.content["identity/TimeInterval"].split("/", 1)]
        return int((end - start).total_seconds())

    def usage_type(self):
        """Parses the "lineItem/UsageType" field to get at the "subtype" (my term).

           Usage types can be of many forms. Here are some examples:

               USE1-USW2-AWS-In-Bytes
               Requests-RBP
               Request
               APN1-DataProcessing-Bytes
               APN1-BoxUsage:c3.2xlarge

           It's a goddamn nightmare. We try our best. Then we return the name of the
           subtype, in the format in which it'll appear in the Graphite metric. Examples
           of usage types are:

               ec2-instance
               ec2-other
               elb
               rds
               
           This method returns None if the usage type isn't known."""
        splut = self.content["lineItem/UsageType"].split("-")
        if len(splut[0]) == 4 and splut[0][0:2] in ("US", "EU", "AP", "SA") and splut[0].isupper() and splut[0][3].isdigit():
            # Stuff before dash was probably a region code like "APN1" or "USW2"
            splut = splut[1:]
        if splut[0].startswith("BoxUsage:"):
            return "ec2-instance"
        return None

    def instance_type(self):
        """Returns the instance type corresponding to a row of the "ec2-instance" usage_type.

           The instance type will have all "." characters replaced with "-" because "." is
           the separator for parts of a Graphite metric name.

           Returns None if no instance type can be parsed (e.g. if this row is not of the
           "ec2-instance" usage type)."""
        if "BoxUsage:" not in self.content["lineItem/UsageType"]:
            return None
        splut = self.content["lineItem/UsageType"].split(":", 1)
        if len(splut) < 2:
            return None
        return splut[1].replace(".", "-")

    def end_time(self):
        return arrow.get(self.content["identity/TimeInterval"].split("/", 1)[1])

    def tags(self):
        return {}

    def amount(self):
        return float(self.content["lineItem/BlendedCost"])


def generate_metrics(csv_file, output_file):
    """Generates metrics from the given CSV and writes them to the given file-like object."""
    reader = csv.reader(csv_file)
    col_names = reader.next()
    formatter = MetricFormatter()
    ledger = MetricLedger([
        ByInstanceType(),
        AllCosts(),
    ])
    logging.info("Calculating billing metrics")
    for row_list in reader:
        row = Row(col_names, row_list)
        # Skip entries of the wrong type
        if row.content["lineItem/LineItemType"] != "Usage": continue
        # Skip non-hourly entries
        if row.interval() != 3600: continue
        ledger.process(row)
    ledger.output(output_file)

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)

    csv_file = open_csv()
    output_file = open_output()
    generate_metrics(csv_file, output_file)
    logging.info("Mission complete.")
