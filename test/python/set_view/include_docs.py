#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
import json
import couchdb
import httplib
import urllib
import common


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



def test_include_docs(params):
    print "Querying map view"
    (map_resp, map_view_result) = common.query(params, "mapview1", {"include_docs": "true"})
    map_etag = map_resp.getheader("ETag")

    assert map_view_result["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(map_view_result["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)

    common.test_keys_sorted(map_view_result)

    for row in map_view_result["rows"]:
        assert "doc" in row, 'row has a "doc" property'
        doc = row["doc"]
        key = row["key"]
        assert doc["integer"] == row["key"], "doc.integer same as row.key"
        assert doc["string"] == str(row["key"]), "doc.string same as String(row.key)"
        assert doc["_rev"].find("1-") == 0, "doc._rev starts with 1-"




def main():
    server = couchdb.Server(url = "http://" + HOST)
    params = {
        "host": HOST,
        "ddoc": DDOC,
        "nparts": NUM_PARTS,
        "ndocs": NUM_DOCS,
        "setname": SET_NAME,
        "server": server
    }

    print "Creating databases"
    common.create_dbs(params)
    common.populate(
        params,
        make_doc = lambda i: {"_id": str(i), "integer": i, "string": str(i)}
        )
    common.define_set_view(params, range(NUM_PARTS), [])
    print "Databases created"

    test_include_docs(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
