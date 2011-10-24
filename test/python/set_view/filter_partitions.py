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



def test(params):
    print "Querying map view with ?partitions=[0,1,3]"
    (map_resp, map_view_result) = common.query(
        params, "mapview1", {"partitions": json.dumps([0, 1, 3])})
    map_etag = map_resp.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)
    assert map_view_result["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(map_view_result["rows"]) == expected, \
        "Query returned %d rows" % (expected,)

    common.test_keys_sorted(map_view_result)

    all_keys = {}
    for r in map_view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d from partition 3 is not in the result" % (key,)

    print "Disabling (making it passive) partition 2"
    common.set_partition_states(params, passive = [1])

    print "Querying map view again with ?partitions=[0,1,3]"
    (map_resp2, map_view_result2) = common.query(
        params, "mapview1", {"partitions": json.dumps([0, 1, 3])})
    map_etag2 = map_resp2.getheader("ETag")

    assert map_view_result2 == map_view_result, "Same result as before"
    assert map_etag2 == map_etag, "Same Etag as before"

    print "Marking partition 2 for cleanup"
    common.set_partition_states(params, cleanup = [1])

    print "Querying map view again with ?partitions=[0,1,3]"
    (map_resp3, map_view_result3) = common.query(
        params, "mapview1", {"partitions": json.dumps([0, 1, 3])})
    map_etag3 = map_resp3.getheader("ETag")

    expected = params["ndocs"] / 2
    assert len(map_view_result3["rows"]) == expected, \
        "Query returned %d rows" % (expected,)
    assert map_etag3 != map_etag2, "Different Etag from before"

    common.test_keys_sorted(map_view_result3)

    all_keys = {}
    for r in map_view_result3["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(2, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d from partition 2 is not in the result" % (key,)
    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d from partition 3 is not in the result" % (key,)

    print "Triggering view compaction (to guarantee cleanup is complete)"
    common.compact_set_view(params)

    print "Querying map view again with ?partitions=[0,1,3]"
    (map_resp4, map_view_result4) = common.query(
        params, "mapview1", {"partitions": json.dumps([0, 1, 3])})
    map_etag4 = map_resp4.getheader("ETag")

    # total_rows is different after cleanup
    assert map_view_result4["rows"] == map_view_result3["rows"], "Same result as before"
    assert map_etag4 == map_etag3, "Same Etag as before"



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

    test(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
