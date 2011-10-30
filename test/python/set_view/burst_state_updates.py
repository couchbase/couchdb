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
NUM_PARTS = 16
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


# Issue a burst of partition state changes, and verify query results and
# index state are correct.
class TestBurstStateUpdates(unittest.TestCase):

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
        # print "\tmaximum of 16 partitions"
        # print "\tactive partitions = []"
        # print "\tpassive partitions = []"
        common.define_set_view(self._params, [], [])


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    def test_burst_state_updates(self):
        state_transitions = [
            {"active": [], "passive": [], "cleanup": []},
            {"active": [0], "passive": [], "cleanup": []},
            {"active": [1], "passive": [], "cleanup": []},
            {"active": [2], "passive": [], "cleanup": []},
            {"active": [3], "passive": [], "cleanup": []},
            {"active": [4], "passive": [], "cleanup": []},
            {"active": [5], "passive": [], "cleanup": []},
            {"active": [6], "passive": [], "cleanup": []},
            {"active": [7], "passive": [], "cleanup": []},
            {"active": [8], "passive": [], "cleanup": []},
            {"active": [9], "passive": [], "cleanup": []},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [10], "passive": [], "cleanup": []},
            {"active": [11], "passive": [], "cleanup": []},
            {"active": [12], "passive": [], "cleanup": []},
            {"active": [13], "passive": [], "cleanup": []},
            {"active": [14], "passive": [], "cleanup": []},
            {"active": [15], "passive": [], "cleanup": []},
            {"active": [], "passive": [], "cleanup": [0]},
            {"active": [], "passive": [], "cleanup": [1]},
            {"active": [], "passive": [], "cleanup": [2]},
            {"active": [], "passive": [], "cleanup": [3]},
            {"active": [], "passive": [], "cleanup": [4]},
            {"active": [], "passive": [], "cleanup": [5]},
            {"active": [], "passive": [], "cleanup": [6]},
            {"active": [], "passive": [], "cleanup": [7]},
            {"active": [], "passive": [0], "cleanup": []},
            {"active": [], "passive": [1], "cleanup": []},
            {"active": [], "passive": [2], "cleanup": []},
            {"active": [], "passive": [3], "cleanup": []},
            {"active": [], "passive": [4], "cleanup": []},
            {"active": [], "passive": [5], "cleanup": []},
            {"active": [], "passive": [6], "cleanup": []},
            {"active": [], "passive": [7], "cleanup": []},
            {"active": [], "passive": [], "cleanup": [8]},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [], "passive": [], "cleanup": [9]},
            {"active": [], "passive": [], "cleanup": [10]},
            {"active": [], "passive": [], "cleanup": [11]},
            {"active": [], "passive": [], "cleanup": [12]},
            {"active": [], "passive": [], "cleanup": [13]},
            {"active": [], "passive": [], "cleanup": [14]},
            {"active": [], "passive": [], "cleanup": [15]},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [0], "passive": [], "cleanup": []},
            {"active": [1], "passive": [], "cleanup": []},
            {"active": [2], "passive": [], "cleanup": []},
            {"active": [3], "passive": [], "cleanup": []},
            {"active": [4], "passive": [], "cleanup": []},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [5], "passive": [], "cleanup": []},
            {"active": [6], "passive": [], "cleanup": []},
            {"active": [7], "passive": [], "cleanup": []},
            {"active": [], "passive": [8], "cleanup": []},
            {"active": [], "passive": [9], "cleanup": []},
            {"active": [], "passive": [10], "cleanup": []},
            {"active": [], "passive": [11], "cleanup": []},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [], "passive": [12], "cleanup": []},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [], "passive": [13], "cleanup": []},
            {"active": [], "passive": [14], "cleanup": []},
            {"active": [], "passive": [15], "cleanup": []},
            {"active": [8], "passive": [], "cleanup": []},
            {"active": [9], "passive": [], "cleanup": []},
            {"active": [10], "passive": [], "cleanup": []},
            {"active": [11], "passive": [], "cleanup": []},
            {"active": [12], "passive": [], "cleanup": []},
            {"active": [13], "passive": [], "cleanup": []},
            {"active": [], "passive": [], "cleanup": []},
            {"active": [14], "passive": [], "cleanup": []},
            {"active": [15], "passive": [], "cleanup": []},
            {"active": [], "passive": [], "cleanup": []},
            ]

        self.do_test_without_updates(state_transitions)
        self.do_test_with_updates(state_transitions)



    # trigger the state changes without updates in between
    def do_test_without_updates(self, state_transitions):

        # print "Triggering state changes without queries in between"
        for state in state_transitions:
            common.set_partition_states(
                params = self._params,
                active = state["active"],
                passive = state["passive"],
                cleanup = state["cleanup"]
                )

        group_stats = self.verify_final_index_state(state_transitions)
        self.assertEqual(group_stats["updater_interruptions"], 0, "0 updater interruptions")
        self.assertEqual(group_stats["updates"], 1, "Got 1 full update")



    # trigger the state changes with updates in between
    # (queries with ?stale=update_after)
    def do_test_with_updates(self, state_transitions):

        # start from a clean state always
        common.set_partition_states(
            params = self._params,
            cleanup = range(self._params["nparts"])
            )
        # print "Cleaning set view via compaction"
        common.compact_set_view(self._params)

        (resp, view_result) = common.query(self._params, "mapview")
        self.assertEqual(view_result["total_rows"], 0, "total_rows is 0 after compaction")
        self.assertEqual(len(view_result["rows"]), 0, "0 rows returned after compaction")
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["update_seqs"], {}, "right update seqs list")

        # print "Triggering state changes with queries (?stale=updater_after) in between"
        for state in state_transitions:
            common.set_partition_states(
                params = self._params,
                active = state["active"],
                passive = state["passive"],
                cleanup = state["cleanup"]
                )
            time.sleep(0.5)
            common.query(self._params, "mapview", {"stale": "update_after", "limit": "100"})

        group_stats = self.verify_final_index_state(state_transitions)
        self.assertTrue(group_stats["updater_interruptions"] > 0, "Got at least 1 updater interruption")
        self.assertEqual(group_stats["updates"], 2, "Got 2 full updates")
        self.assertEqual(group_stats["compactions"], 1, "Got 1 compaction")
        self.assertTrue(group_stats["cleanups"] >= 1, "Got at least 1 full cleanup")



    def verify_final_index_state(self, state_transitions):
        # print "Verifying final view state"
        doc_count = common.set_doc_count(self._params)
        (resp, view_result) = common.query(self._params, "mapview")

        # print "Final view result (%d rows):  %s" % \
        #    (len(view_result["rows"]), json.dumps(view_result, sort_keys = True, indent = 4))

        self.assertEqual(view_result["total_rows"], doc_count,
                         "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(view_result["rows"]), doc_count,
                         "Query returned %d rows" % doc_count)

        self.assertEqual(view_result["rows"][0]["key"], 1, "First key is 1")
        self.assertEqual(view_result["rows"][-1]["key"], doc_count, "Last key is %d" % doc_count)
        common.test_keys_sorted(view_result)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        # print "Final group info:  %s" % json.dumps(info, sort_keys = True, indent = 4)
        self.assertEqual(
            info["active_partitions"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            "right active partitions list"
            )
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))

        return info["stats"]
