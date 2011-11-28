#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
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
NUM_DOCS = 200000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


class TestPassivePartitionsUpdate(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._params = {
            "host": HOST,
            "ddoc": DDOC,
            "nparts": NUM_PARTS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME,
            "server": couchdb.Server(url = "http://" + HOST)
            }


    @classmethod
    def tearDownClass(cls):
        # print "Deleting test data"
        common.create_dbs(cls._params, True)


    def test_updates(self):
        self.do_test_updates()
        self.do_test_set_passive_partitions_when_updater_is_running()


    # Verify that we have both active and passive partitions, the updater
    # first indexes the active partitions and then after it indexes the
    # documents from the passive partitions. Clients querying with ?stale=false
    # are unblocked as soon as the indexing of the active partitions is done.
    # This test verifies that property.
    def do_test_updates(self):
        # print "Creating databases"
        common.create_dbs(self._params)
        common.populate(self._params)
        # print "Databases created"
        # print "Configuring set view with 1 active partition and 3 passive partitions"
        common.define_set_view(self._params, [0], [1, 2, 3])

        # print "Querying map view"
        (resp, view_result) = common.query(self._params, "mapview", {"limit": "20"})

        # print "Grabbing group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [1, 2, 3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["update_seqs"]["0"], (self._params["ndocs"] / 4), "All data from partition 1 was indexed")

        sum_passive_seqs = 0
        for i in [1, 2, 3]:
            sum_passive_seqs += info["update_seqs"][str(i)]

        self.assertTrue(sum_passive_seqs < (self._params["ndocs"] - (self._params["ndocs"] / 4)),
                        "Passive partitions are still being indexed")
        self.assertEqual(info["updater_running"], True, "View updater still running")
        self.assertEqual(info["updater_state"], "updating_passive", "View updater in state 'updating_passive'")

        # print "Verifying view query response"
        self.assertEqual(len(view_result["rows"]), 20, "Query returned 20 rows")

        common.test_keys_sorted(view_result)
        self.assertEqual(view_result["rows"][0]["key"], 1, "First row has key 1")
        last_key_expected = range(1, self._params["ndocs"], self._params["nparts"])[19]
        self.assertEqual(view_result["rows"][-1]["key"], last_key_expected, "Last row has key %d" % last_key_expected)

        # print "Waiting for view updater to finish"
        while True:
            info = common.get_set_view_info(self._params)
            if not info["updater_running"]:
                break
            else:
                time.sleep(3)

        # print "Grabbing group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [1, 2, 3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "Right update seq for partition %d" % (i + 1))
        self.assertEqual(info["updater_running"], False, "View updater not running anymore")
        self.assertEqual(info["updater_state"], "not_running", "View updater not running anymore")

        # print "Querying view again and validating the response"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(len(view_result["rows"]), (self._params["ndocs"] / 4), "Query returned %d rows" % (self._params["ndocs"] / 4))

        common.test_keys_sorted(view_result)
        self.assertEqual(view_result["rows"][0]["key"], 1, "First row has key 1")
        last_key_expected = range(1, self._params["ndocs"], self._params["nparts"])[-1]
        self.assertEqual(view_result["rows"][-1]["key"], last_key_expected, "Last row has key %d" % last_key_expected)


    def do_test_set_passive_partitions_when_updater_is_running(self):
        # print "Re-creating databases"
        common.create_dbs(self._params)
        common.populate(self._params)
        # print "Configuring set view with all partitions active"
        common.define_set_view(self._params, [0, 1, 2, 3], [])

        # print "Querying map view in steady state with ?stale=update_after"
        (resp, view_result) = common.query(self._params, "mapview", {"stale": "update_after"})

        self.assertEqual(len(view_result["rows"]), 0, "Received empty row set")
        self.assertEqual(view_result["total_rows"], 0, "Received empty row set")

        # print "Marking partition 4 as passive"
        common.set_partition_states(self._params, passive = [3])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Waiting for the set view updater to finish"
        iterations = 0
        while True:
            info = common.get_set_view_info(self._params)
            if info["updater_running"]:
                iterations += 1
            else:
                break

        self.assertTrue(iterations > 0, "Updater was running when partition 4 was set to passive")
        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [],  "right cleanup partitions list")

        # print "Querying map view again"
        (resp, view_result) = common.query(self._params, "mapview")

        doc_count = common.set_doc_count(self._params)
        expected_row_count = common.set_doc_count(self._params, [0, 1, 2])
        self.assertEqual(view_result["total_rows"], doc_count, "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(view_result["rows"]), expected_row_count, "Query returned %d rows" % expected_row_count)

        common.test_keys_sorted(view_result)

        all_keys = {}
        for r in view_result["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 4 was set to passive" % key)

        # print "Adding 2 new documents to partition 4"
        server = self._params["server"]
        db4 = server[self._params["setname"] + "/3"]
        new_doc1 = {"_id": "999999999", "integer": 999999999, "string": "999999999"}
        new_doc2 = {"_id": "000", "integer": -1111, "string": "000"}
        db4.save(new_doc1)
        db4.save(new_doc2)

        # print "Querying map view again"
        (resp, view_result2) = common.query(self._params, "mapview")
        self.assertEqual(view_result2["rows"], view_result["rows"], "Same result set as before")

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [],  "right cleanup partitions list")

        total_doc_count = common.set_doc_count(self._params)
        # print "Changing partition 4 from passive to active"
        common.set_partition_states(self._params, active = [3])

        # print "Querying map view again"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(view_result["total_rows"], total_doc_count, "total_rows is %d" % total_doc_count)
        self.assertEqual(len(view_result["rows"]), total_doc_count, "number of rows returned is %d" % total_doc_count)
        common.test_keys_sorted(view_result)

        self.assertEqual(view_result["rows"][0]["key"], new_doc2["integer"], "new_doc2 reflected in result set at first row")
        self.assertEqual(view_result["rows"][-1]["key"], new_doc1["integer"], "new_doc1 reflected in result set at last row")

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [],  "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))
