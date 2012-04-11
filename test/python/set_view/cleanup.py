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
NUM_PARTS = 4
NUM_DOCS = 400000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


class TestCleanup(unittest.TestCase):

    def setUp(self):
        self._params = {
            "host": HOST,
            "ddoc": DDOC,
            "nparts": NUM_PARTS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME,
            "server": couchdb.Server(url = "http://" + HOST)
            }
        # print "Creating databases"
        common.create_dbs(self._params)
        common.populate(self._params)
        common.define_set_view(self._params, range(NUM_PARTS), [])
        # print "Databases created"


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    def test_cleanup(self):
        self.do_test_simple_cleanup()
        self.do_test_set_cleanup_partitions_when_updater_is_running()
        self.do_test_change_partition_states_while_cleanup_running()


    def do_test_simple_cleanup(self):
        # print "Querying view in steady state"
        (resp, view_result) = common.query(self._params, "mapview1")
        etag = resp.getheader("ETag")

        self.assertEqual(view_result["total_rows"], self._params["ndocs"],
                         "Query returned %d total_rows" % self._params["ndocs"])
        self.assertEqual(len(view_result["rows"]), self._params["ndocs"],
                         "Query returned %d rows" % self._params["ndocs"])

        common.test_keys_sorted(view_result)

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))

        # print "Triggering partition 4 cleanup"
        common.set_partition_states(self._params, cleanup = [3])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [3], "right cleanup partitions list")
        for i in [0, 1, 2]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertFalse("3" in info["update_seqs"], "partition 3 not in info.update_seqs")
        self.assertEqual(info["cleanup_running"], True, "cleanup process is running")

        # print "Querying view again"
        (resp2, view_result2) = common.query(self._params, "mapview1")
        etag2 = resp2.getheader("ETag")

        expected = self._params["ndocs"] - (self._params["ndocs"] / 4)

        self.assertEqual(view_result2["total_rows"], self._params["ndocs"],
                         "Query returned %d total_rows" % self._params["ndocs"])
        self.assertEqual(len(view_result2["rows"]), expected, "Query returned %d rows" % expected)
        self.assertNotEqual(etag2, etag, "Different Etag after cleanup triggered")

        common.test_keys_sorted(view_result2)

        all_keys = {}
        for r in view_result2["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 4 cleanup triggered" % key)

        # print "Triggering view compaction and querying view again"
        common.compact_set_view(self._params)

        (resp3, view_result3) = common.query(self._params, "mapview1")
        etag3 = resp3.getheader("ETag")

        expected = self._params["ndocs"] - (self._params["ndocs"] / 4)

        self.assertEqual(view_result3["total_rows"], expected, "Query returned %d total_rows" % expected)
        self.assertEqual(len(view_result3["rows"]), expected, "Query returned %d rows" % expected)
        self.assertEqual(etag2, etag3, "Same Etag after cleanup finished")

        common.test_keys_sorted(view_result3)

        all_keys = {}
        for r in view_result3["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 4 cleanup finished" % key)

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertFalse("3" in info["update_seqs"], "partition 3 not in info.update_seqs")

        # print "Adding 2 new documents to partition 4"
        server = self._params["server"]
        db4 = server[self._params["setname"] + "/3"]
        new_doc1 = {"_id": "999999999", "integer": 999999999, "string": "999999999"}
        new_doc2 = {"_id": "000", "integer": -1111, "string": "000"}
        db4.save(new_doc1)
        db4.save(new_doc2)

        # print "Querying view again"
        (resp4, view_result4) = common.query(self._params, "mapview1")
        etag4 = resp4.getheader("ETag")

        expected = self._params["ndocs"] - (self._params["ndocs"] / 4)
        self.assertEqual(view_result4["total_rows"], expected, "Query returned %d total_rows" % expected)
        self.assertEqual(len(view_result4["rows"]), expected, "Query returned %d rows" % expected)
        self.assertEqual(etag4, etag3, "Same etag after adding new documents to cleaned partition")

        common.test_keys_sorted(view_result4)

        all_keys = {}
        for r in view_result4["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 4 cleanup finished" % key)
        self.assertFalse(new_doc1["integer"] in all_keys, "new_doc1 not in query result after cleanup")
        self.assertFalse(new_doc2["integer"] in all_keys, "new_doc2 not in query result after cleanup")

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertFalse("3" in info["update_seqs"], "partition 3 not in info.update_seqs")

        # print "Triggering compaction again and verifying it doesn't crash"
        common.compact_set_view(self._params)
        (resp5, view_result5) = common.query(self._params, "mapview1")
        etag5 = resp5.getheader("ETag")

        self.assertEqual(etag5, etag4, "Same etag after second compaction")
        self.assertEqual(view_result5, view_result4, "Same query results after second compaction")

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertFalse("3" in info["update_seqs"], "partition 3 not in info.update_seqs")


    def do_test_set_cleanup_partitions_when_updater_is_running(self):
        # print "Marking all partitions for cleanup"
        common.set_partition_states(self._params, cleanup = range(self._params["nparts"]))

        # print "Compacting the set view group"
        common.compact_set_view(self._params)

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Querying view"
        (resp, view_result) = common.query(self._params, "mapview1")
        self.assertEqual(view_result["total_rows"], 0, "Empty view result")
        self.assertEqual(len(view_result["rows"]), 0, "Empty view result")

        # print "Marking all partitions as active"
        common.set_partition_states(self._params, active = range(self._params["nparts"]))

        # print "Querying view with ?stale=update_after"
        (resp, view_result) = common.query(self._params, "mapview1", {"stale": "update_after"})
        self.assertEqual(view_result["total_rows"], 0, "Empty view result")
        self.assertEqual(len(view_result["rows"]), 0, "Empty view result")

        # print "Marking partition 2 for cleanup while the updater is running"
        common.set_partition_states(self._params, cleanup = [1])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [1], "right cleanup partitions list")
        self.assertFalse("1" in info["update_seqs"], "partition 1 not in info.update_seqs")
        self.assertFalse("1" in info["purge_seqs"], "partition 1 not in info.update_seqs")

        # print "Waiting for the set view updater to finish"
        iterations = 0
        while True:
            info = common.get_set_view_info(self._params)
            if info["updater_running"]:
                iterations += 1
            else:
                break

        self.assertTrue(iterations > 0, "Updater was running when partition 2 was marked for cleanup")
        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertTrue(["cleanup_partitions"] == [1] or info["cleanup_partitions"] == [],
                        "cleanup partitions list is not wrong")
        self.assertFalse("1" in info["update_seqs"], "partition 1 not in info.update_seqs")
        self.assertFalse("1" in info["purge_seqs"], "partition 1 not in info.update_seqs")

        # print "Querying view"
        (resp, view_result) = common.query(self._params, "mapview1")

        doc_count = common.set_doc_count(self._params, [0, 2, 3])
        self.assertEqual(len(view_result["rows"]), doc_count, "Query returned %d rows" % doc_count)
        common.test_keys_sorted(view_result)

        all_keys = {}
        for r in view_result["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(2, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 2 marked for cleanup" % key)

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertFalse("1" in info["update_seqs"], "partition 1 not in info.update_seqs")
        self.assertFalse("1" in info["purge_seqs"], "partition 1 not in info.update_seqs")



    def do_test_change_partition_states_while_cleanup_running(self):
        # print "Marking all partitions as active"
        common.set_partition_states(self._params, active = range(self._params["nparts"]))

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        # print "Updating view"
        (resp, view_result) = common.query(self._params, "mapview1", {"limit": "100"})

        self.assertEqual(view_result["total_rows"], doc_count, "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(view_result["rows"]), 100, "Query returned 100 rows")
        common.test_keys_sorted(view_result)

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            expected = common.set_doc_count(self._params, [i])
            self.assertEqual(info["update_seqs"][str(i)], expected,
                             "right update seq for partition %d" % (i + 1))

        # print "Marking partitions 1 and 2 for cleanup"
        common.set_partition_states(self._params, cleanup = [0, 1])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["cleanup_running"], True, "cleanup is running")
        self.assertEqual(info["active_partitions"], [2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [0, 1], "right cleanup partitions list")

        # print "Marking partitions 1 and 2 as active while cleanup is ongoing"
        common.set_partition_states(self._params, active = [0, 1])

        info = common.get_set_view_info(self._params)
        self.assertEqual(type(info["pending_transition"]), dict, "pending_transition is an object")
        self.assertEqual(sorted(info["pending_transition"]["active"]),
                         [0, 1],
                         "pending_transition active list is [0, 1]")
        self.assertEqual(info["pending_transition"]["passive"],
                         [],
                         "pending_transition passive list is []")
        self.assertEqual(info["pending_transition"]["cleanup"],
                         [],
                         "pending_transition cleanup list is []")

        # print "Waiting for pending transition to be applied"
        iterations = 0
        while True:
            if iterations > 600:
                raise(Exception("timeout waiting for pending transition to be applied"))
            info = common.get_set_view_info(self._params)
            if info["pending_transition"] is None:
                break
            else:
                time.sleep(1)
                iterations += 1

        # print "Querying view"
        (resp, view_result) = common.query(self._params, "mapview1")
        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3])

        info = common.get_set_view_info(self._params)
        self.assertEqual(view_result["total_rows"], doc_count, "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(view_result["rows"]), doc_count, "Query returned %d rows" % doc_count)
        common.test_keys_sorted(view_result)

        all_keys = {}
        for r in view_result["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(1, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys,
                            "Key %d in result after partition 1 activated" % key)
        for key in xrange(2, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys,
                            "Key %d in result after partition 2 activated" % key)
        for key in xrange(3, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys,
                            "Key %d (partition 3) in result set" % key)
        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertTrue(key in all_keys,
                            "Key %d (partition 4) in result set" % key)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            expected = common.set_doc_count(self._params, [i])
            self.assertEqual(info["update_seqs"][str(i)], expected,
                             "right update seq for partition %d" % (i + 1))

