#!/usr/bin/env python
"""Turns an hourly billing CSV into one we can test against.

   We redact anything proprietary, including:

       * tag names and values
       * cost values
       * instance IDs
       * line item IDs
       * account IDs
       
   We write the redacted CSV to stdout."""

import sys
import csv
import random

from awsbill2graphite import Row

ALPHA = "abcdefghijklmnopqrstuvwxyz"

INCLUDED_COLS = set((
    "identity/TimeInterval",
    "lineItem/LineItemType",
    "product/location",
    "product/volumeType",
))

def make_alpha(n):
    """Returns a lowercase alphabetic string n characters long."""
    global ALPHA
    return "".join((random.choice(ALPHA) for i in range(n)))

def make_instance_type(instance_type):
    """Returns a random instance type string of the same kind as the given one.

       For example, if instance_type is "db.r3.large", we'll return an instance
       type starting with "db."."""
    splut = instance_type.split(".")
    splut[-2] = random.choice(("t2", "c4", "m4"))
    splut[-1] = random.choice(("medium", "large", "2xlarge"))
    return ".".join(splut)

if __name__ == "__main__":
    reader = csv.reader(open(sys.argv[1], "rb"))
    writer = csv.writer(sys.stdout)
    col_names = reader.next()

    # Redact tag names
    for i in range(len(col_names)):
        if col_names[i].startswith("resourceTags/user:"):
            col_names[i] = "resourceTags/user:{0}".format(make_alpha(10))
    writer.writerow(col_names)

    for row_list in reader:
        row = []
        for i in range(len(row_list)):
            col_name = col_names[i]
            col_val = row_list[i]

            if col_name in INCLUDED_COLS:
                row.append(col_val)
            elif col_name.endswith("Cost"):
                row.append(round(random.random()*10., 8))
            elif col_name.startswith("resourceTags/user:"):
                row.append(col_val)
            elif col_name == "lineItem/UsageType" and "Usage:" in col_val:
                splut = col_val.rsplit(":", 1) 
                splut[-1] = make_instance_type(splut[-1])
                row.append(":".join(splut))
            elif col_name == "lineItem/UsageType":
                row.append(col_val)
            else:
                row.append("")
        writer.writerow(row)
