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



def test_staleness(params):
    print "Querying map view"
    (map_resp, map_view_result) = common.query(params, "mapview1")
    map_etag = map_resp.getheader("ETag")

    assert map_view_result["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(map_view_result["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)

    common.test_keys_sorted(map_view_result)

    print "Adding 1 new document to each partition"
    server = params["server"]
    db1 = server[params["setname"] + "/0"]
    db2 = server[params["setname"] + "/1"]
    db3 = server[params["setname"] + "/2"]
    db4 = server[params["setname"] + "/3"]
    new_doc1 = {"_id": "new_doc_1", "integer": -1, "string": "1"}
    new_doc2 = {"_id": "new_doc_2", "integer": -2, "string": "2"}
    new_doc3 = {"_id": "new_doc_3", "integer": -3, "string": "3"}
    new_doc4 = {"_id": "new_doc_4", "integer": -4, "string": "4"}

    db1.save(new_doc1)
    db2.save(new_doc2)
    db3.save(new_doc3)
    db4.save(new_doc4)

    print "Querying map view with ?stale=ok"
    (map_resp2, map_view_result2) = common.query(params, "mapview1", {"stale": "ok"})
    map_etag2 = map_resp2.getheader("ETag")

    assert map_view_result2 == map_view_result, "Same response as before with ?stale=ok"
    assert map_etag2 == map_etag, "Same etag as before with ?stale=ok"

    print "Querying map view with ?stale=update_after"
    (map_resp3, map_view_result3) = common.query(params, "mapview1", {"stale": "update_after"})
    map_etag3 = map_resp3.getheader("ETag")

    assert map_view_result3 == map_view_result, "Same response as before with ?stale=update_after"
    assert map_etag3 == map_etag, "Same etag as before with ?stale=updater_after"

    time.sleep(5)

    print "Querying map view without stale option"
    (map_resp4, map_view_result4) = common.query(params, "mapview1")
    map_etag4 = map_resp4.getheader("ETag")

    assert map_view_result4["total_rows"] == (params["ndocs"] + 4), \
        "Query returned %d total_rows" % (params["ndocs"] + 4,)
    assert len(map_view_result4["rows"]) == (params["ndocs"] + 4), \
        "Query returned %d rows" % (params["ndocs"] + 4,)
    assert map_etag4 != map_etag3, "New etag after ?stale=update_after"

    common.test_keys_sorted(map_view_result4)

    assert map_view_result4["rows"][0]["id"] == new_doc4["_id"], "new_doc4 in view result"
    assert map_view_result4["rows"][1]["id"] == new_doc3["_id"], "new_doc3 in view result"
    assert map_view_result4["rows"][2]["id"] == new_doc2["_id"], "new_doc2 in view result"
    assert map_view_result4["rows"][3]["id"] == new_doc1["_id"], "new_doc1 in view result"




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
    common.populate(params)
    common.define_set_view(params, range(NUM_PARTS), [])
    print "Databases created"

    test_staleness(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
