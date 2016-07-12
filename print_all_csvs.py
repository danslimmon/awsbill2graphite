#!/usr/bin/env python
import gzip
import json
import os
import shutil
import sys
import tempfile

import boto3

def all_s3_primary_manifests(objects):
    """Returns the S3 object(s) corresponding to all primary manifests.

       `objects` should be an iterable of S3 objects."""
    manifests = [o for o in objects if o.key.endswith("Manifest.json")]
    # The primary manifest(s) will be the one(s) with the shortest path length
    manifests.sort(key=lambda a: len(a.key))
    n_slash = manifests[0].key.count("/")
    for i in range(len(manifests)-1):
        if manifests[i].key.count("/") > n_slash:
            break
    return manifests[:i]


def print_all_from_s3(s3_path, tempdir, region_name):
    """Outputs all hourly billing reports from the given S3 path to stdout."""
    s3 = boto3.resource("s3", region_name=region_name)
    bucket = s3.Bucket(s3_path.split("/")[2])
    primaries = all_s3_primary_manifests(bucket.objects.all())

    # Now we parse the manifest to get the path to the latest billing CSV
    s3_csvs = []
    for pri in primaries:
        manifest = json.loads(pri.get()['Body'].read())
        s3_csvs.extend(manifest["reportKeys"])

    # Download each billing CSV to a temp directory and decompress
    header_written = False
    for s3_csv in s3_csvs:
        local_path = os.path.join(tempdir, s3_csv.split("/")[-1])
        local_file = open(local_path, "w")
        obj = [o for o in bucket.objects.filter(Prefix=s3_csv)][0]
        local_file.write(obj.get()['Body'].read())
        local_file.close()

        with gzip.open(local_path, "r") as f:
            for line in f:
                if line.startswith(
                        "identity/LineItemId,"
                ) and header_written:
                    continue
                sys.stdout.write(line)
                header_written = True
        # Remove these files as we finish with them to save on disk space
        os.unlink(local_path)

if __name__ == "__main__":
    if os.getenv("REGION_NAME") != '':
        region_name = os.getenv("REGION_NAME")
    else:
        region_name = 'us-west-1'

    tempdir = tempfile.mkdtemp(".awsbill")
    print_all_from_s3(os.getenv("AWSBILL_REPORT_PATH"), tempdir, region_name)
    shutil.rmtree(tempdir)
