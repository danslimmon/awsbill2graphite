import csv
import random
import unittest
from datetime import datetime

import awsbill2graphite as a2g

class LedgerTest(unittest.TestCase):
    def setUp(self):
        ledger = a2g.new_metric_ledger()
        reader = csv.reader(open("test_data/hourly_billing-1.csv", "rb"))
        col_names = reader.next()
        for row_list in reader:
            row = a2g.Row(col_names, row_list)
            ledger.process(row)
        self.timeseries = ledger.get_timeseries()

    def assert_timeseries_equal(self, metric_name, expected, received):
        """Determines whether the two given timeseries dicts are equal (within a tolerance)."""
        for k in expected.keys():
            if not received.has_key(k):
                self.fail("Key {0} missing from received timeseries '{1}'".format(k, metric_name))
                return
            if abs(expected[k] - received[k]) > .00001:
                self.fail("Value for {0} for received timeseries {1} is {2}; should be {3}".format(
                    k, metric_name, expected[k], received[k]))
                return
        for k in received.keys():
            if not expected.has_key(k):
                self.fail("Unexpected key {0} in received timeseries '{1}'".format(k, metric_name))
                return

    def testTsInstanceType(self):
        self.assertTrue(self.timeseries.has_key("us-west-1.ec2-instance.m4-2xlarge"))
        self.assert_timeseries_equal(
            "us-west-1.ec2-instance.m4-2xlarge",
            self.timeseries["us-west-1.ec2-instance.m4-2xlarge"],
            {
                datetime.fromtimestamp(1459746000): 31.497950,
                datetime.fromtimestamp(1459764000): 26.083113,
                datetime.fromtimestamp(1459782000): 61.615628,
                datetime.fromtimestamp(1459800000): 63.319794,
                datetime.fromtimestamp(1459789200): 42.888862,
                datetime.fromtimestamp(1459807200): 33.440607,
                datetime.fromtimestamp(1459753200): 49.640219,
                datetime.fromtimestamp(1459771200): 47.892134,
                datetime.fromtimestamp(1459735200): 43.360197,
                datetime.fromtimestamp(1459814400): 84.484617,
                datetime.fromtimestamp(1459760400): 55.846821,
                datetime.fromtimestamp(1459778400): 29.705564,
                datetime.fromtimestamp(1459742400): 63.989894,
                datetime.fromtimestamp(1459796400): 54.198285,
                datetime.fromtimestamp(1459731600): 47.450255,
                datetime.fromtimestamp(1459749600): 77.140611,
                datetime.fromtimestamp(1459803600): 78.267747,
                datetime.fromtimestamp(1459767600): 61.143072,
                datetime.fromtimestamp(1459785600): 39.729129,
                datetime.fromtimestamp(1459810800): 48.819524,
                datetime.fromtimestamp(1459774800): 44.610415,
                datetime.fromtimestamp(1459792800): 19.039679,
                datetime.fromtimestamp(1459738800): 41.609403,
                datetime.fromtimestamp(1459756800): 47.254336,
            }
        )
        self.assertTrue(self.timeseries.has_key("ap-northeast-1.ec2-instance.t2-medium"))
        self.assert_timeseries_equal(
            "ap-northeast-1.ec2-instance.t2-medium",
            self.timeseries["ap-northeast-1.ec2-instance.t2-medium"],
            {
                datetime.fromtimestamp(1459807200): 9.228804,
                datetime.fromtimestamp(1459753200): 5.313574,
                datetime.fromtimestamp(1459771200): 4.238844,
                datetime.fromtimestamp(1459746000): 13.584161,
                datetime.fromtimestamp(1459764000): 5.319477,
                datetime.fromtimestamp(1459760400): 13.284314,
                datetime.fromtimestamp(1459778400): 8.792418,
                datetime.fromtimestamp(1459742400): 4.248000,
                datetime.fromtimestamp(1459735200): 3.921303,
                datetime.fromtimestamp(1459796400): 9.269115,
                datetime.fromtimestamp(1459810800): 13.109077,
                datetime.fromtimestamp(1459803600): 15.168434,
                datetime.fromtimestamp(1459785600): 3.440763,
                datetime.fromtimestamp(1459782000): 4.131503,
                datetime.fromtimestamp(1459800000): 8.607207,
                datetime.fromtimestamp(1459774800): 2.417751,
                datetime.fromtimestamp(1459814400): 5.292426,
                datetime.fromtimestamp(1459756800): 6.206031,
            }
        )

    def testS3PrimaryManifest(self):
        class _S3Obj:
            def __init__(self, k): self.key = k
            # Make sure sorting the objects without `key=` causes test to fail
            def __lt__(self, o): return (random.randint(0, 1) == 0)

        manifests = [_S3Obj(k) for k in [
            "prefix/hourly_billing/20160201-20160301/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160301-20160401/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160401-20160501/11c0a000-107e-11e6-813f-881fa1019b9e/hourly_billing-1.csv.gz",
            "prefix/hourly_billing/20160401-20160501/11c0a000-107e-11e6-813f-881fa1019b9e/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160501-20160601/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160401-20160501/21b3c44a-107e-11e6-8355-881fa1019b9e/hourly_billing-1.csv.gz",
            "prefix/hourly_billing/20160401-20160501/21b3c44a-107e-11e6-8355-881fa1019b9e/hourly_billing-2.csv.gz",
            "prefix/hourly_billing/20160401-20160501/21b3c44a-107e-11e6-8355-881fa1019b9e/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160501-20160601/2e6de863-107e-11e6-97e6-881fa1019b9e/hourly_billing-1.csv.gz",
            "prefix/hourly_billing/20160501-20160601/2e6de863-107e-11e6-97e6-881fa1019b9e/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160401-20160501/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160501-20160601/3d1ed007-107e-11e6-acd4-881fa1019b9e/hourly_billing-1.csv.gz",
            "prefix/hourly_billing/20160501-20160601/3d1ed007-107e-11e6-acd4-881fa1019b9e/hourly_billing-Manifest.json",
            "prefix/hourly_billing/20160301-20160401/fbc0aa99-1083-11e6-918b-881fa1019b9e/hourly_billing-1.csv.gz",
            "prefix/hourly_billing/20160301-20160401/fbc0aa99-1083-11e6-918b-881fa1019b9e/hourly_billing-Manifest.json",
        ]]
        primaries = a2g.s3_primary_manifests(manifests)
        self.assertEqual(primaries[0].key,
                        "prefix/hourly_billing/20160401-20160501/hourly_billing-Manifest.json")
        self.assertEqual(primaries[1].key,
                        "prefix/hourly_billing/20160501-20160601/hourly_billing-Manifest.json")
