#!/usr/bin/python

try: import simplejson as json
except ImportError: import json
import couchdb
import httplib
import urllib
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
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


class TestUpdates(unittest.TestCase):

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
        # print "Databases created"
        # print "Configuring set view with:"
        # print "\tmaximum of 8 partitions"
        # print "\tactive partitions = [0, 1, 2, 3, 4, 5]"
        # print "\tpassive partitions = []"
        common.define_set_view(self._params, [0, 1, 2, 3, 4, 5], [])


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    # Test adding new active partitions while the updater is running.
    def test_updates(self):
        total_doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        # print "Querying view in steady state with ?stale=update_after"
        (resp, view_result) = common.query(self._params, "mapview", {"stale": "update_after"})

        self.assertEqual(len(view_result["rows"]), 0, "Received empty row set")
        self.assertEqual(view_result["total_rows"], 0, "Received empty row set")

        # print "Adding partitions 7 and 8 as active while updater is running"
        common.set_partition_states(self._params, active = [6, 7])

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")

        # print "Waiting for updater to finish"
        count = 0
        while True:
            info = common.get_set_view_info(self._params)
            if info["updater_running"]:
                count += 1
            else:
                break

        self.assertTrue(count > 0, "Updater was running when partitions 7 and 8 were added as active")

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in [0, 1, 2, 3, 4, 5]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))

        # print "Querying view again (steady state)"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(len(view_result["rows"]), total_doc_count, "number of received rows is %d" % total_doc_count)
        self.assertEqual(view_result["total_rows"], total_doc_count, "total_rows is %d" % total_doc_count)
        common.test_keys_sorted(view_result)

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
        self.assertEqual(new_total_doc_count, (total_doc_count + len(new_docs)), "8 documents were added")
        total_doc_count = new_total_doc_count

        # print "Querying view again (steady state)"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(len(view_result["rows"]), total_doc_count, "number of received rows is %d" % total_doc_count)
        self.assertEqual(view_result["total_rows"], total_doc_count, "total_rows is %d" % total_doc_count)
        common.test_keys_sorted(view_result)

        i = len(new_docs) - 1
        j = len(view_result["rows"]) - 1
        while i >= 0:
            self.assertEqual(view_result["rows"][j]["key"], new_docs[i]["integer"],
                             "new document %s in view result set" % new_docs[i]["_id"])
            self.assertEqual(view_result["rows"][j]["id"], new_docs[i]["_id"],
                             "new document %s in view result set" % new_docs[i]["_id"])
            i -= 1
            j -= 1

        # print "Deleting the documents that were added before"
        i = 0
        for doc in new_docs:
            db = self._params["setname"] + "/" + str(i)
            common.delete_doc(self._params, db, doc["_id"])
            i = (i + 1) % self._params["nparts"]

        new_total_doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(new_total_doc_count, (total_doc_count - len(new_docs)), "8 documents were deleted")

        # print "Querying view again (steady state)"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(len(view_result["rows"]), new_total_doc_count,
                         "number of received rows is %d" % new_total_doc_count)
        self.assertEqual(view_result["total_rows"], new_total_doc_count,
                         "total_rows is %d" % new_total_doc_count)
        common.test_keys_sorted(view_result)

        all_ids = {}
        all_keys = {}
        for row in view_result["rows"]:
            all_ids[row["id"]] = True
            all_keys[row["key"]] = True

        for doc in new_docs:
            self.assertFalse(doc["_id"] in all_ids, "deleted doc %s not in view results anymore")
            self.assertFalse(doc["integer"] in all_keys, "deleted doc %s not in view results anymore")
