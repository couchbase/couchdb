#!/usr/bin/python

try: import simplejson as json
except ImportError: import json
import couchdb
import httplib
import urllib
import time
import common
import unittest


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view"
NUM_PARTS = 8
NUM_DOCS = 800000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


class TestUpdateCleanup(unittest.TestCase):

    def setUp(self):
        self._params = {
            "host": HOST,
            "ddoc": DDOC,
            "nparts": NUM_PARTS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME,
            "server": couchdb.Server(url = "http://" + HOST)
            }
        # print "Creating %d databases, %d documents per database" % (NUM_PARTS, NUM_DOCS / NUM_PARTS)
        common.create_dbs(self._params)
        common.populate(self._params)
        # print "Databases created"
        # print "Configuring set view with:"
        # print "\tmaximum of 8 partitions"
        # print "\tactive partitions = [0, 1, 2, 3]"
        # print "\tpassive partitions = []"
        common.define_set_view(self._params, [0, 1, 2, 3], [])


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    # Test that the updater also does a cleanup.
    def test_update_cleanup(self):
        # print "Triggering initial view update"
        t0 = time.time()
        (resp, view_result) = common.query(self._params, "mapview", {"limit": "1"})
        # print "Update took %.2f seconds" % (time.time() - t0)

        # print "Verifying group info"

        info = common.get_set_view_info(self._params)
        stats = info["stats"]

        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(stats["updates"], 1, "1 full update done so far")
        self.assertEqual(stats["updater_interruptions"], 0, "no updater interruptions so far")
        self.assertEqual(stats["cleanups"], 0, "0 cleanups done")
        self.assertEqual(stats["cleanup_interruptions"], 0, "no cleanup interruptions so far")

        # print "Adding new partitions 5, 6, 7 and 8 and marking partitions 1 and 4 for cleanup"
        common.set_partition_states(self._params, active = [4, 5, 6, 7], cleanup = [0, 3])

        # print "Querying view (should trigger update + cleanup)"
        t0 = time.time()
        (resp, view_result) = common.query(self._params, "mapview")
        t1 = time.time()

        # print "Verifying query results"

        expected_total = common.set_doc_count(self._params, [1, 2, 4, 5, 6, 7])
        self.assertEqual(view_result["total_rows"], expected_total, "total rows is %d" % expected_total)
        self.assertEqual(len(view_result["rows"]), expected_total, "got %d tows" % expected_total)
        common.test_keys_sorted(view_result)

        all_keys = {}
        for r in view_result["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(1, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key %d (partition 1) not in query result after update+cleanup" % key)
        for key in xrange(2, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key %d (partition 2) in query result after update+cleanup" % key)
        for key in xrange(3, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key %d (partition 3) in query result after update+cleanup" % key)
        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key %d (partition 4) not in query result after update+cleanup" % key)
        for key in xrange(5, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key %d (partition 5) in query result after update+cleanup" % key)
        for key in xrange(6, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key %d (partition 6) in query result after update+cleanup" % key)
        for key in xrange(7, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key %d (partition 7) in query result after update+cleanup" % key)
        for key in xrange(8, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key %d (partition 8) in query result after update+cleanup" % key)

        # print "Verifying group info"

        info = common.get_set_view_info(self._params)
        stats = info["stats"]

        self.assertEqual(info["active_partitions"], [1, 2, 4, 5, 6, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(stats["updates"], 2, "2 full updates done so far")
        self.assertEqual(stats["updater_interruptions"], 0, "no updater interruptions so far")
        self.assertEqual(stats["cleanups"], 1, "1 full cleanup done")
        self.assertEqual(stats["cleanup_interruptions"], 1, "1 cleanup interruption done")

        for i in info["active_partitions"]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertTrue(str(i) in info["update_seqs"], "partition %d in info.update_seqs" % (i + 1))
            self.assertEqual(info["update_seqs"][str(i)], expected_seq, "right seq in info.update_seqs[%d]" % i)

        # print "Update+cleanup took %.2f seconds, cleanup took %.2f seconds" % \
        #    ((t1 - t0), stats["last_cleanup_duration"])



