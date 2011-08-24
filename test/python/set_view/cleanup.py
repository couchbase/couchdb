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
SET_NAME = "test_suite_set_view"
NUM_PARTS = 4
NUM_DOCS = 100000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


def test_cleanup(params):
    print "Querying view in steady state"
    (resp, view_result) = common.query(params, "mapview1")
    etag = resp.getheader("ETag")

    assert view_result["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)

    common.test_keys_sorted(view_result)

    print "Triggering partition 4 cleanup and querying view again"
    common.cleanup_partition(params, 3)

    (resp2, view_result2) = common.query(params, "mapview1")
    etag2 = resp2.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert view_result2["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result2["rows"]) == expected, \
        "Query returned %d rows" % (expected,)
    assert etag2 != etag, "Different Etag after cleanup triggered"

    common.test_keys_sorted(view_result2)

    all_keys = {}
    for r in view_result2["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 cleanup triggered" % (key,)

    print "Triggering view compaction and querying view again"
    common.compact_set_view(params)

    (resp3, view_result3) = common.query(params, "mapview1")
    etag3 = resp3.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert view_result3["total_rows"] == expected, \
        "Query returned %d total_rows" % (expected,)
    assert len(view_result3["rows"]) == expected, \
        "Query returned %d rows" % (expected,)
    assert etag2 == etag3, "Same Etag after cleanup finished"

    common.test_keys_sorted(view_result3)

    all_keys = {}
    for r in view_result3["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 cleanup finished" % (key,)

    print "Adding 2 new documents to partition 4"
    server = params["server"]
    db4 = server[params["setname"] + "/3"]
    new_doc1 = {"_id": "999999999", "integer": 999999999, "string": "999999999"}
    new_doc2 = {"_id": "000", "integer": -1111, "string": "000"}
    db4.save(new_doc1)
    db4.save(new_doc2)

    print "Querying view again"

    (resp4, view_result4) = common.query(params, "mapview1")
    etag4 = resp4.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert view_result4["total_rows"] == expected, \
        "Query returned %d total_rows" % (expected,)
    assert len(view_result4["rows"]) == expected, \
        "Query returned %d rows" % (expected,)
    assert etag4 == etag3, "Same etag after adding new documents to cleaned partition"

    common.test_keys_sorted(view_result4)

    all_keys = {}
    for r in view_result4["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 cleanup finished" % (key,)
    assert not(new_doc1["integer"] in all_keys), "new_doc1 not in query result after cleanup"
    assert not(new_doc2["integer"] in all_keys), "new_doc2 not in query result after cleanup"

    print "Triggering compaction again and verifying it doesn't crash"
    common.compact_set_view(params)
    (resp5, view_result5) = common.query(params, "mapview1")
    etag5 = resp5.getheader("ETag")

    assert etag5 == etag4, "Same etag after second compaction"
    assert view_result5 == view_result4, "Same query results after second compaction"



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

    test_cleanup(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
