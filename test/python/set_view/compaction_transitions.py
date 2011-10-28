#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
import json
import couchdb
import httplib
import urllib
import time
import common
import unittest


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view_compact"
NUM_PARTS = 8
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


class TestCompactionTransitions(unittest.TestCase):

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
        # print "Creating databases"
        common.create_dbs(cls._params)
        common.populate(cls._params)
        # print "Configuring set view with:"
        # print "\tmaximum of 8 partitions"
        # print "\tactive partitions = [0, 1, 2, 3, 4]"
        # print "\tpassive partitions = []"
        common.define_set_view(cls._params, [0, 1, 2, 3], [])


    @classmethod
    def tearDownClass(cls):
        # print "Deleting test data"
        common.create_dbs(cls._params, True)


    def test_compaction_transitions(self):
        self.do_test_set_passive_during_compaction()
        self.do_test_set_active_during_compaction()


    def do_test_set_passive_during_compaction(self):
        # print "Querying map view"
        (map_resp, map_view_result) = common.query(self._params, "mapview1")

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(map_view_result["total_rows"], doc_count,
                         "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(map_view_result["rows"]), doc_count,
                         "Query returned %d rows" % doc_count)

        common.test_keys_sorted(map_view_result)

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            db = self._params["server"][self._params["setname"] + "/" + str(i)]
            seq = db.info()["update_seq"]
            self.assertEqual(info["update_seqs"][str(i)], seq,
                             "right update seq for partition %d" % (i + 1))

        # print "Triggering view compaction"
        common.compact_set_view(self._params, False)

        # print "Marking partition 4 as passive"
        common.set_partition_states(self._params, passive = [3])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Waiting for compaction to finish"
        compaction_was_running = (common.wait_set_view_compaction_complete(self._params) > 0)
        self.assertTrue(compaction_was_running, "Compaction was running when the view update was triggered")

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Querying map view again"
        (map_resp, map_view_result) = common.query(self._params, "mapview1")

        expected = common.set_doc_count(self._params, [0, 1, 2])
        self.assertEqual(map_view_result["total_rows"], doc_count,
                         "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(map_view_result["rows"]), expected,
                         "Query returned %d rows" % expected)

        common.test_keys_sorted(map_view_result)

        all_keys = {}
        for r in map_view_result["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 4 set to passive" % key)

        # print "Triggering view compaction"
        common.compact_set_view(self._params, False)

        # print "Adding two new partitions, 5 and 6, as passive while compaction is running"
        common.set_partition_states(self._params, passive = [4, 5])

        # print "Waiting for compaction to finish"
        compaction_was_running = (common.wait_set_view_compaction_complete(self._params) > 0)
        self.assertTrue(compaction_was_running, "Compaction was running when the view update was triggered")

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3, 4, 5], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3, 4, 5]:
            self.assertTrue(str(i) in info["update_seqs"], "%d in info.update_seqs" % i)
        for i in [6, 7]:
            self.assertFalse(str(i) in info["update_seqs"], "%d not in info.update_seqs" % i)

        # print "Querying map view again"
        (map_resp, map_view_result2) = common.query(self._params, "mapview1")
        self.assertEqual(map_view_result2["rows"],  map_view_result["rows"], "Same result set as before")

        total_doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        # print "Adding 1 new document to each partition"
        new_docs = []
        for i in [0, 1, 2, 3, 4, 5, 6, 7]:
            server = self._params["server"]
            db = self._params["server"][self._params["setname"] + "/" + str(i)]
            value = total_doc_count + i + 1
            new_doc = {
                "_id": str(value),
                "integer": value,
                "string": str(value)
                }
            new_docs.append(new_doc)
            db.save(new_doc)

        new_total_doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(new_total_doc_count, (total_doc_count + 8), "8 documents added")
        self.assertEqual(len(new_docs), 8, "8 documents added")

        info = common.get_set_view_info(self._params)
        if info["updater_running"]:
            # print "Waiting for updater to finish"
            self.assertEqual(info["updater_state"], "updating_passive",
                             "updater state is updating_passive")
            while True:
                info = common.get_set_view_info(self._params)
                if info["updater_running"]:
                    self.assertEqual(info["updater_state"], "updating_passive",
                                     "updater state is updating_passive")
                    time.sleep(3)
                else:
                    break

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2])
        expected_total_rows = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5])

        # print "Querying map view again"
        (map_resp, map_view_result) = common.query(self._params, "mapview1")

        self.assertEqual(len(map_view_result["rows"]), expected_row_count, "len(rows) is %d" % expected_row_count)
        common.test_keys_sorted(map_view_result)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [3, 4, 5], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3, 4, 5]:
            self.assertTrue(str(i) in info["update_seqs"], "%d in info.update_seqs" % i)
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "info.update_seqs[%d] is %d" % (i, expected_seq))



    def do_test_set_active_during_compaction(self):
        # print "Triggering view compaction"
        common.compact_set_view(self._params, False)

        # print "Marking partitions 4, 5 and 6 as active while compaction is running"
        common.set_partition_states(self._params, active = [3, 4, 5])

        # print "Adding new partitions 7 and 8 with active state while compaction is running"
        common.set_partition_states(self._params, active = [6, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Querying map view while compaction is running"
        (map_resp, map_view_result) = common.query(self._params, "mapview1", {"limit": "10"})

        self.assertEqual(len(map_view_result["rows"]), 10, "Query returned 10 rows")
        common.test_keys_sorted(map_view_result)

        # print "Waiting for compaction to finish"
        compaction_was_running = (common.wait_set_view_compaction_complete(self._params) > 0)
        self.assertTrue(compaction_was_running, "Compaction was running when the view update was triggered")

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Querying map view again"
        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        (map_resp, map_view_result) = common.query(self._params, "mapview1")

        self.assertEqual(map_view_result["total_rows"], doc_count,
                         "Query returned %d total_rows" % doc_count)
        self.assertEqual(len(map_view_result["rows"]), doc_count,
                         "Query returned %d rows" % doc_count)

        common.test_keys_sorted(map_view_result)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3, 4, 5, 6, 7]:
            self.assertTrue(str(i) in info["update_seqs"], "%d in info.update_seqs" % i)
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "info.update_seqs[%d] is %d" % (i, expected_seq))

