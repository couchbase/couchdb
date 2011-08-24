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
        },
        "redview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }",
            "reduce": "_count"
        }
    }
}


def test_compaction(params):
    print "Querying map view"
    (map_resp, map_view_result) = common.query(params, "mapview1")
    map_etag = map_resp.getheader("ETag")

    assert map_view_result["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(map_view_result["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)

    common.test_keys_sorted(map_view_result)

    print "Querying reduce view"
    (red_resp, red_view_result) = common.query(params, "redview1")
    red_etag = red_resp.getheader("ETag")

    assert len(red_view_result["rows"]) == 1, "Query returned 1 row"
    assert red_view_result["rows"][0]["value"] == params["ndocs"], \
        "Non-grouped reduce value is %d" % (params["ndocs"],)

    print "Triggering view compaction"
    common.compact_set_view(params)

    print "Querying map view"
    (map_resp2, map_view_result2) = common.query(params, "mapview1")
    map_etag2 = map_resp2.getheader("ETag")

    assert map_view_result2 == map_view_result, "Got same map response after compaction"
    assert map_etag2 == map_etag, "Same map view etag after compaction"

    print "Querying reduce view"
    (red_resp2, red_view_result2) = common.query(params, "redview1")
    red_etag2 = red_resp2.getheader("ETag")

    assert red_view_result2 == red_view_result, "Got same reduce response after compaction"
    assert red_etag2 == red_etag, "Same reduce view etag after compaction"

    print "Adding 2 new documents to partition 4"
    server = params["server"]
    db4 = server[params["setname"] + "/3"]
    new_doc1 = {"_id": "9999999999999", "integer": 999999999999999999, "string": "999999999"}
    new_doc2 = {"_id": "000", "integer": -1111, "string": "000"}
    db4.save(new_doc1)
    db4.save(new_doc2)

    print "Triggering view compaction"
    common.compact_set_view(params, False)

    print "Triggering set view group index update"
    common.query(params, "redview1")

    print "Waiting for set view compaction to finish"
    compaction_was_running = (common.wait_set_view_compaction_complete(params) > 0)
    assert compaction_was_running, "Compaction was running when the view update was triggered"

    print "Querying map view"
    (map_resp3, map_view_result3) = common.query(params, "mapview1")
    map_etag3 = map_resp3.getheader("ETag")

    assert map_view_result3["total_rows"] == (params["ndocs"] + 2), \
        "Query returned %d total_rows" % (params["ndocs"] + 2,)
    assert len(map_view_result3["rows"]) == (params["ndocs"] + 2), \
        "Query returned %d rows" % (params["ndocs"] + 2,)
    assert map_etag3 != map_etag, "Different map view etag after recompaction"

    common.test_keys_sorted(map_view_result3)
    assert map_view_result3["rows"][0]["id"] == new_doc2["_id"], "new_doc2 in map view"
    assert map_view_result3["rows"][-1]["id"] == new_doc1["_id"], "new_doc1 in map view"

    print "Querying reduce view"
    (red_resp3, red_view_result3) = common.query(params, "redview1")
    red_etag3 = red_resp3.getheader("ETag")

    assert red_view_result3["rows"][0]["value"] == (params["ndocs"] + 2), \
        "Non-grouped reduce value is %d" % (params["ndocs"] + 2,)
    assert red_etag3 != red_etag, "Different reduce view etag after compaction"

    print "Triggering another view compaction"
    common.compact_set_view(params, False)

    print "Triggering partition 4 cleanup while compaction is ongoing"
    common.cleanup_partition(params, 3)

    compaction_was_running = (common.wait_set_view_compaction_complete(params) > 0)
    assert compaction_was_running, "Compaction was running when the cleanup was triggered"

    print "Querying map view"
    (map_resp4, map_view_result4) = common.query(params, "mapview1")
    map_etag4 = map_resp4.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert map_view_result4["total_rows"] == expected, \
        "Query returned %d total_rows after recompact+cleanup" % (expected,)
    assert len(map_view_result4["rows"]) == expected, \
        "Query returned %d rows after recompact+cleanup" % (expected,)
    assert map_etag4 != map_etag, "Different map view etag after compaction+cleanup"

    common.test_keys_sorted(map_view_result4)

    all_keys = {}
    for r in map_view_result4["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 cleanup triggered" % (key,)

    assert not(new_doc1["integer"] in all_keys), "new_doc1 not included in view query result"
    assert not(new_doc2["integer"] in all_keys), "new_doc2 not included in view query result"
    assert map_view_result4 != map_view_result3, "Different map view result after recompact+cleanup"

    print "Querying reduce view"
    (red_resp4, red_view_result4) = common.query(params, "redview1")
    red_etag4 = red_resp4.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert red_view_result4["rows"][0]["value"] == expected, \
        "Non-grouped reduce value is %d after recompact+cleanup" % (params["ndocs"] + 2,)
    assert red_etag4 != red_etag3, "Different reduce view etag after compaction+cleanup"



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

    test_compaction(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
