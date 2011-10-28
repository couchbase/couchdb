#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
import json
import couchdb
import httplib
import urllib
import common
import time
import unittest


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view_compact"
NUM_PARTS = 4
NUM_DOCS = 20000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}



class TestStale(unittest.TestCase):

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


    def test_staleness(self):
        # print "Querying map view"
        (map_resp, map_view_result) = common.query(self._params, "mapview1")
        map_etag = map_resp.getheader("ETag")

        self.assertEqual(map_view_result["total_rows"], self._params["ndocs"],
                         "Query returned %d total_rows" % self._params["ndocs"])
        self.assertEqual(len(map_view_result["rows"]), self._params["ndocs"],
                         "Query returned %d rows" % self._params["ndocs"])

        common.test_keys_sorted(map_view_result)

        # To give the set view group some time to write and fsync the index header
        time.sleep(5)

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info["update_seqs"][str(i)], (self._params["ndocs"] / 4),
                             "right update seq for partition %d" % (i + 1))

        # print "Adding 1 new document to each partition"
        server = self._params["server"]
        db1 = server[self._params["setname"] + "/0"]
        db2 = server[self._params["setname"] + "/1"]
        db3 = server[self._params["setname"] + "/2"]
        db4 = server[self._params["setname"] + "/3"]
        new_doc1 = {"_id": "new_doc_1", "integer": -1, "string": "1"}
        new_doc2 = {"_id": "new_doc_2", "integer": -2, "string": "2"}
        new_doc3 = {"_id": "new_doc_3", "integer": -3, "string": "3"}
        new_doc4 = {"_id": "new_doc_4", "integer": -4, "string": "4"}

        db1.save(new_doc1)
        db2.save(new_doc2)
        db3.save(new_doc3)
        db4.save(new_doc4)

        # print "Querying map view with ?stale=ok"
        (map_resp2, map_view_result2) = common.query(self._params, "mapview1", {"stale": "ok"})
        map_etag2 = map_resp2.getheader("ETag")

        self.assertEqual(map_view_result2, map_view_result, "Same response as before with ?stale=ok")
        self.assertEqual(map_etag2, map_etag, "Same etag as before with ?stale=ok")

        # print "Verifying set view group info"
        info2 = common.get_set_view_info(self._params)
        self.assertEqual(info2, info, "Same set view group info after ?stale=ok query")

        # print "Querying map view with ?stale=update_after"
        (map_resp3, map_view_result3) = common.query(self._params, "mapview1", {"stale": "update_after"})
        map_etag3 = map_resp3.getheader("ETag")

        self.assertEqual(map_view_result3, map_view_result, "Same response as before with ?stale=update_after")
        self.assertEqual(map_etag3, map_etag, "Same etag as before with ?stale=updater_after")

        time.sleep(5)

        # print "Verifying set view group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3]:
            self.assertEqual(info["update_seqs"][str(i)], ((self._params["ndocs"] / 4) + 1),
                             "right update seq for partition %d" % (i + 1))

        # print "Querying map view without stale option"
        (map_resp4, map_view_result4) = common.query(self._params, "mapview1")
        map_etag4 = map_resp4.getheader("ETag")

        self.assertEqual(map_view_result4["total_rows"], (self._params["ndocs"] + 4),
                         "Query returned %d total_rows" % (self._params["ndocs"] + 4))
        self.assertEqual(len(map_view_result4["rows"]), (self._params["ndocs"] + 4),
                         "Query returned %d rows" % (self._params["ndocs"] + 4))
        self.assertNotEqual(map_etag4, map_etag3, "New etag after ?stale=update_after")

        common.test_keys_sorted(map_view_result4)

        self.assertEqual(map_view_result4["rows"][0]["id"], new_doc4["_id"], "new_doc4 in view result")
        self.assertEqual(map_view_result4["rows"][1]["id"], new_doc3["_id"], "new_doc3 in view result")
        self.assertEqual(map_view_result4["rows"][2]["id"], new_doc2["_id"], "new_doc2 in view result")
        self.assertEqual(map_view_result4["rows"][3]["id"], new_doc1["_id"], "new_doc1 in view result")

