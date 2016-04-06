# awsbill2graphite

Converts AWS hourly billing CSVs to Graphite metrics

## Usage

First set the following environment variables:

* `AWS_ACCESS_KEY_ID`: The identifier for an AWS credentials pair that will enable access
  to the bucket with billing reports in it
* `AWS_SECRET_ACCESS_KEY`: The secret access key that corresponds to `AWS_ACCESS_KEY_ID`
* `AWSBILL_S3_PREFIX`: The S3 path where the reports live (this should be `s3://` followed
  by the bucket name followed by the "Report path" as defined in the AWS billing control
  panel.
* `AWSBILL_GRAPHITE_URL`: The URL of the Graphite server to which to write metrics. If
  instead you want to output metrics to stdout, set this environment variable to `stdout`.
* `AWSBILL_METRIC_PREFIX`: The prefix to use for metrics written to Graphite.
* `AWSBILL_TAGS`: The (comma-separated) tags to produce metrics for. If you don't want
  to produce metrics for any tags, leave this environment variable empty.

Then run

    awsbill2graphite.py

This will produce metrics like so:

    PREFIX.REGION.ec2-instance.t2.micro
    PREFIX.REGION.ec2-instance.c4.2xlarge
    PREFIX.REGION.ec2-other.snapshot-storage
    PREFIX.REGION.ec2-other.piops
    PREFIX.REGION.rds.m4.medium

Each metric will have a data point every hour. This data point represents the total amount
charged to your account for the hour _previous_ to the data point's timestamp.
