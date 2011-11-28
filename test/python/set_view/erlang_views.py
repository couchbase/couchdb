#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
try: import simplejson as json
except ImportError: import json
import couchdb
import httplib
import urllib
import common
import unittest


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view"
NUM_PARTS = 8
NUM_DOCS = 200000
DDOC = {
    "_id": "_design/test",
    "language": "erlang",
    "views": {
        "mapview": {
            "map": "fun({Doc}) -> " + \
                'K = couch_util:get_value(<<"integer">>, Doc, null),' + \
                'V = couch_util:get_value(<<"string">>, Doc, null),' + \
                'Emit(K, V)' + \
                'end.'
        }
    }
}


class TestErlangViews(unittest.TestCase):

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
        active_parts = range(self._params["nparts"])
        common.define_set_view(self._params, active_parts, [])
        common.set_config_parameter(
            self._params,
            "native_query_servers",
            "erlang",
            "{couch_native_process, start_link, []}"
            )


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    def test_erlang_views(self):
        total_doc_count = common.set_doc_count(self._params, range(self._params["nparts"]))

        # print "Querying map view in steady state"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(view_result["total_rows"], total_doc_count,
                         "Query returned %d total_rows" % total_doc_count)
        self.assertEqual(len(view_result["rows"]), total_doc_count,
                         "Query returned %d rows" % total_doc_count)
        common.test_keys_sorted(view_result)

        # print "Adding 1 new document to each partition"
        new_docs = []
        for i in xrange(self._params["nparts"]):
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

        new_total_doc_count = common.set_doc_count(self._params, range(self._params["nparts"]))
        self.assertEqual(new_total_doc_count, (total_doc_count + len(new_docs)), "N documents were added")
        total_doc_count = new_total_doc_count

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

        new_total_doc_count = common.set_doc_count(self._params, range(self._params["nparts"]))
        self.assertEqual(new_total_doc_count, (total_doc_count - len(new_docs)), "documents deleted")

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
