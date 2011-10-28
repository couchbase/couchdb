#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
import json
import couchdb
import httplib
import urllib
import common
import unittest


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view_compact"
NUM_PARTS = 4
NUM_DOCS = 100000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        },
        "redview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }",
            "reduce": "_count"
        }
    }
}


class TestCompaction(unittest.TestCase):

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
        common.define_set_view(cls._params, range(NUM_PARTS), [])
        # print "Databases created"


    @classmethod
    def tearDownClass(cls):
        # print "Deleting test data"
        common.create_dbs(cls._params, True)


    def test_compaction(self):
        # print "Querying map view"
        (map_resp, map_view_result) = common.query(self._params, "mapview1")
        map_etag = map_resp.getheader("ETag")

        self.assertEqual(map_view_result["total_rows"], self._params["ndocs"],
                         "Query returned %d total_rows" % self._params["ndocs"])
        self.assertEqual(len(map_view_result["rows"]), self._params["ndocs"],
                         "Query returned %d rows" % self._params["ndocs"])

        common.test_keys_sorted(map_view_result)

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))

        # print "Querying reduce view"
        (red_resp, red_view_result) = common.query(self._params, "redview1")
        red_etag = red_resp.getheader("ETag")

        self.assertEqual(len(red_view_result["rows"]), 1, "Query returned 1 row")
        self.assertEqual(red_view_result["rows"][0]["value"], self._params["ndocs"],
                         "Non-grouped reduce value is %d" % self._params["ndocs"])

        # print "Triggering view compaction"
        common.compact_set_view(self._params)

        # print "Verifying set view group info"
        info2 = common.get_set_view_info(self._params)
        self.assertEqual(info2["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info2["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info2["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info2["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertTrue(info2["disk_size"] < info["disk_size"], "Smaller disk_size after compaction")

        # print "Querying map view"
        (map_resp2, map_view_result2) = common.query(self._params, "mapview1")
        map_etag2 = map_resp2.getheader("ETag")

        self.assertEqual(map_view_result2, map_view_result, "Got same map response after compaction")
        self.assertEqual(map_etag2, map_etag, "Same map view etag after compaction")

        # print "Querying reduce view"
        (red_resp2, red_view_result2) = common.query(self._params, "redview1")
        red_etag2 = red_resp2.getheader("ETag")

        self.assertEqual(red_view_result2, red_view_result, "Got same reduce response after compaction")
        self.assertEqual(red_etag2, red_etag, "Same reduce view etag after compaction")

        # print "Adding 2 new documents to partition 4"
        server = self._params["server"]
        db4 = server[self._params["setname"] + "/3"]
        new_doc1 = {"_id": "9999999999999", "integer": 999999999999999999, "string": "999999999"}
        new_doc2 = {"_id": "000", "integer": -1111, "string": "000"}
        db4.save(new_doc1)
        db4.save(new_doc2)

        # print "Triggering view compaction"
        common.compact_set_view(self._params, False)

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertEqual(info["compact_running"], True, "Compaction is flagged as running in group info")


        # print "Triggering set view group index update"
        common.query(self._params, "redview1")

        # print "Waiting for set view compaction to finish"
        compaction_was_running = (common.wait_set_view_compaction_complete(self._params) > 0)
        self.assertTrue(compaction_was_running, "Compaction was running when the view update was triggered")

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            if i == 3:
                seq = (self._params["ndocs"] / 4) + 2
            else:
                seq = (self._params["ndocs"] / 4)
            self.assertEqual(info["update_seqs"][str(i)], seq, "right update seq for partition %d" % (i + 1))

        # print "Querying map view"
        (map_resp3, map_view_result3) = common.query(self._params, "mapview1")
        map_etag3 = map_resp3.getheader("ETag")

        self.assertEqual(map_view_result3["total_rows"], (self._params["ndocs"] + 2),
                         "Query returned %d total_rows" % (self._params["ndocs"] + 2))
        self.assertEqual(len(map_view_result3["rows"]), (self._params["ndocs"] + 2),
                         "Query returned %d rows" % (self._params["ndocs"] + 2))
        self.assertNotEqual(map_etag3, map_etag, "Different map view etag after recompaction")

        common.test_keys_sorted(map_view_result3)
        self.assertEqual(map_view_result3["rows"][0]["id"], new_doc2["_id"], "new_doc2 in map view")
        self.assertEqual(map_view_result3["rows"][-1]["id"], new_doc1["_id"], "new_doc1 in map view")

        # print "Querying reduce view"
        (red_resp3, red_view_result3) = common.query(self._params, "redview1")
        red_etag3 = red_resp3.getheader("ETag")

        self.assertEqual(red_view_result3["rows"][0]["value"], (self._params["ndocs"] + 2),
                         "Non-grouped reduce value is %d" % (self._params["ndocs"] + 2))
        self.assertNotEqual(red_etag3, red_etag, "Different reduce view etag after compaction")

        # print "Triggering another view compaction"
        common.compact_set_view(self._params, False)

        # print "Triggering partition 4 cleanup while compaction is ongoing"
        common.set_partition_states(self._params, cleanup = [3])

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [3], "right cleanup partitions list")
        for i in [0, 1, 2]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertFalse("3" in info["update_seqs"], "paritition 4 not in group info update_seqs")
        self.assertEqual(info["compact_running"], True, "Compaction is flagged as running in group info")
        self.assertEqual(info["cleanup_running"], True, "Cleanup is flagged as running in group info")

        compaction_was_running = (common.wait_set_view_compaction_complete(self._params) > 0)
        self.assertTrue(compaction_was_running, "Compaction was running when the cleanup was triggered")

        # print "Verifying set view group info"
        info2 = common.get_set_view_info(self._params)
        self.assertEqual(info2["active_partitions"], [0, 1, 2], "right active partitions list")
        self.assertEqual(info2["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info2["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2]:
            self.assertEqual(info2["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))
        self.assertFalse("3" in info["update_seqs"], "paritition 4 not in group info update_seqs")
        self.assertTrue(info2["disk_size"] < info["disk_size"], "Smaller disk_size after compaction+cleanup")

        # print "Querying map view"
        (map_resp4, map_view_result4) = common.query(self._params, "mapview1")
        map_etag4 = map_resp4.getheader("ETag")

        expected = self._params["ndocs"] - (self._params["ndocs"] / 4)

        self.assertEqual(map_view_result4["total_rows"], expected,
                         "Query returned %d total_rows after recompact+cleanup" % expected)
        self.assertEqual(len(map_view_result4["rows"]), expected,
                         "Query returned %d rows after recompact+cleanup" % expected)
        self.assertNotEqual(map_etag4, map_etag, "Different map view etag after compaction+cleanup")

        common.test_keys_sorted(map_view_result4)

        all_keys = {}
        for r in map_view_result4["rows"]:
            all_keys[r["key"]] = True

        for key in xrange(4, self._params["ndocs"], self._params["nparts"]):
            self.assertFalse(key in all_keys,
                             "Key %d not in result after partition 4 cleanup triggered" % key)

        self.assertFalse(new_doc1["integer"] in all_keys, "new_doc1 not included in view query result")
        self.assertFalse(new_doc2["integer"] in all_keys, "new_doc2 not included in view query result")
        self.assertNotEqual(map_view_result4, map_view_result3, "Different map view result after recompact+cleanup")

        # print "Querying reduce view"
        (red_resp4, red_view_result4) = common.query(self._params, "redview1")
        red_etag4 = red_resp4.getheader("ETag")

        expected = self._params["ndocs"] - (self._params["ndocs"] / 4)

        self.assertEqual(red_view_result4["rows"][0]["value"], expected,
                         "Non-grouped reduce value is %d after recompact+cleanup" % (self._params["ndocs"] + 2))
        self.assertNotEqual(red_etag4, red_etag3, "Different reduce view etag after compaction+cleanup")
 
