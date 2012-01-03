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
NUM_PARTS = 4
NUM_DOCS = 40000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


class TestIncludeDocs(unittest.TestCase):

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


    def test_include_docs(self):
        # print "Querying map view"
        (map_resp, map_view_result) = common.query(self._params, "mapview1", {"include_docs": "true"})
        map_etag = map_resp.getheader("ETag")

        self.assertEqual(map_view_result["total_rows"], self._params["ndocs"],
                         "Query returned %d total_rows" % self._params["ndocs"])
        self.assertEqual(len(map_view_result["rows"]), self._params["ndocs"],
                         "Query returned %d rows" % self._params["ndocs"])

        common.test_keys_sorted(map_view_result)

        for row in map_view_result["rows"]:
            self.assertTrue("doc" in row, 'row has a "doc" property')
            doc = row["doc"]
            self.assertEqual(doc["integer"], row["key"], "doc.integer same as row.key")
            self.assertEqual(doc["string"], str(row["key"]), "doc.string same as String(row.key)")
