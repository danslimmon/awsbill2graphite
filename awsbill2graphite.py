#!/usr/bin/env python
import os
import sys
import csv
import logging

def open_csv():
    """Opens the latest hourly billing CSV file. Returns an open file object.
    
       Depending on the AWSBILL_REPORT_PATH environment variable, this may involve
       downloading from S3, or it may just open a local file."""
    report_path = os.getenv("AWSBILL_REPORT_PATH")
    if report_path.startswith("file://"):
        csv_path = report_path[len("file://")-1:]
    elif report_path.startswith("s3://"):
        raise NotImplementedError("S3 downloads not implemented yet")
    else:
        raise ValueError("AWSBILL_REPORT_PATH environment variable must start with 'file://' or 's3://'")
    return open(csv_path)

def open_output():
    """Opens the file-like object that will be used for output, and returns it.

       Depending on the AWSBILL_GRAPHITE_URL environment variable, writes to this
       object may be sent to a Graphite server or they may be written to stdout."""
    output_url = os.getenv("AWSBILL_GRAPHITE_URL")
    if output_url == "stdout":
        output_file = sys.stdout
    elif output_url.startswith("http://") or output_url.startswith("https://"):
        raise NotImplementedError("Writing to Graphite not implemented yet")
    else:
        raise ValueError("AWSBILL_GRAPHITE_URL environment variable must specify an HTTP or HTTPS URL, or be set to 'stdout'")
    return output_file


if __name__ == "__main__":
    try:
        csv_file = open_csv()
        output_channel = open_output()
    except Exception, e:
        print(e)
        sys.exit(1)
