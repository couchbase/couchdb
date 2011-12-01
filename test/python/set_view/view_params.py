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
SET_NAME = "test_suite_set_view_params"
NUM_PARTS = 4
NUM_DOCS = 40000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        },
        "redview1": {
            "map": "function(doc) { emit(doc.integer, 1); }",
            "reduce": "_count"
        }
    }
}


class TestViewParams(unittest.TestCase):

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
        common.populate(
            self._params,
            make_doc = lambda i: {"_id": str(i), "integer": i, "string": str(i)}
            )
        common.define_set_view(self._params, range(NUM_PARTS), [])
        # print "Databases created"


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    def test_view_params(self):
        self.do_test_keys_param()


    def do_test_keys_param(self):
        # print "Running test do_test_keys_param"
        keys = [1111, 4, self._params["ndocs"] * 5, 7]

        # print "Querying map view with ?keys=%s" % json.dumps(keys)
        (map_resp2, map_view_result2) = common.query(
            self._params, "mapview1", {"include_docs": "true", "keys": json.dumps(keys)})

        self.assertEqual(len(map_view_result2["rows"]), 3, "Query returned 3 rows")

        all_keys = {}
        for row in map_view_result2["rows"]:
            all_keys[row["key"]] = True

        for key in keys:
            if key > self._params["ndocs"]:
                self.assertFalse(key in all_keys, "Key %d not in result" % key)
            else:
                self.assertTrue(key in all_keys, "Key %d in result" % key)

        # print "Querying reduce view with ?keys=%s" % json.dumps(keys)
        (red_resp, red_view_result) = common.query(
            self._params, "redview1", {"keys": json.dumps(keys), "group": "true"})

        self.assertEqual(len(red_view_result["rows"]), 3, "Query returned 3 rows")

        all_keys = {}
        for row in red_view_result["rows"]:
            all_keys[row["key"]] = row["value"]

        for key in keys:
            if key > self._params["ndocs"]:
                self.assertFalse(key in all_keys, "Key %d not in result" % key)
            else:
                self.assertTrue(key in all_keys, "Key %d in result" % key)
                self.assertEqual(all_keys[key], 1, "Key %d has value 1" % key)
