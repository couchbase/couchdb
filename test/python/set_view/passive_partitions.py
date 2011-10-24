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
        },
        "redview1": {
            "map": "function(doc) { emit(doc.integer, doc.string); }",
            "reduce": "function(keys, values, rereduce) {" + \
                "if (rereduce) {" + \
                "    return sum(values);" + \
                "} else {" + \
                "    return values.length;" + \
                "}" + \
             "}"
        },
        "redview2": {
            "map": "function(doc) { emit(doc.integer, doc.string); }",
            "reduce": "_count"
        }
    }
}


def test_maps(params):
    print "Querying map view in steady state"
    (resp, view_result) = common.query(params, "mapview1")
    etag = resp.getheader("ETag")

    assert view_result["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)

    common.test_keys_sorted(view_result)

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Setting partition 4 as passive"
    common.set_partition_states(params, passive = [3])

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Querying view again"
    (resp2, view_result2) = common.query(params, "mapview1")
    etag2 = resp2.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert view_result2["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result2["rows"]) == expected, \
        "Query returned %d rows" % (expected,)
    assert etag2 != etag, "Different Etag after setting passive partition"

    common.test_keys_sorted(view_result2)

    all_keys = {}
    for r in view_result2["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 was made passive" % (key,)

    print "Setting partition 4 state to active"
    common.set_partition_states(params, active = [3])

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Querying view again"
    (resp3, view_result3) = common.query(params, "mapview1")
    etag3 = resp3.getheader("ETag")

    assert view_result3["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result3["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)
    assert etag3 == etag, "ETag is same as first query response"

    common.test_keys_sorted(view_result3)

    print "Setting partitions 1 and 4 to passive state"
    common.set_partition_states(params, passive = [0, 3])

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [1, 2], "right active partitions list"
    assert info["passive_partitions"] == [0, 3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Querying view again"
    (resp4, view_result4) = common.query(params, "mapview1")
    etag4 = resp4.getheader("ETag")

    expected = params["ndocs"] / 2

    assert view_result4["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result4["rows"]) == expected, \
        "Query returned %d rows" % (expected,)
    assert etag4 != etag, "ETag is different from all previous responses"
    assert etag4 != etag2, "ETag is different from all previous responses"

    common.test_keys_sorted(view_result4)

    all_keys = {}
    for r in view_result4["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(1, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 1 was made passive" % (key,)
    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 was made passive" % (key,)

    print "Marking all partitions as passive and querying view again"
    common.set_partition_states(params, passive = range(params["nparts"]))

    (resp5, view_result5) = common.query(params, "mapview1")
    etag5 = resp5.getheader("ETag")

    assert view_result5["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result5["rows"]) == 0, "Query returned 0 rows"
    assert etag5 != etag, "ETag is different from all previous responses"
    assert etag5 != etag2, "ETag is different from all previous responses"
    assert etag5 != etag3, "ETag is different from all previous responses"
    assert etag5 != etag4, "ETag is different from all previous responses"

    print "Setting all partitions to active state"
    common.set_partition_states(params, active = range(params["nparts"]))

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Querying view again"
    (resp6, view_result6) = common.query(params, "mapview1")
    etag6 = resp6.getheader("ETag")

    assert view_result6["total_rows"] == params["ndocs"], \
        "Query returned %d total_rows" % (params["ndocs"],)
    assert len(view_result6["rows"]) == params["ndocs"], \
        "Query returned %d rows" % (params["ndocs"],)
    assert etag6 == etag, "ETag is the same from first view query response"

    common.test_keys_sorted(view_result6)



def test_reduces(params, viewname):
    print "Querying reduce view in steady state"
    (resp, view_result) = common.query(params, viewname)
    etag = resp.getheader("ETag")

    assert len(view_result["rows"]) == 1, "Query returned 1 row"
    assert view_result["rows"][0]["value"] == params["ndocs"], \
        "Non-grouped reduce value is %d" % (params["ndocs"],)

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Setting partition 3 to passive state"
    common.set_partition_states(params, passive = [2])

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 3], "right active partitions list"
    assert info["passive_partitions"] == [2], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Querying reduce view again"
    (resp2, view_result2) = common.query(params, viewname)
    etag2 = resp2.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)
    assert len(view_result2["rows"]) == 1, "Query returned 1 row"
    assert view_result2["rows"][0]["value"] == expected, \
        "Non-grouped reduce value is %d" % (expected,)
    assert etag2 != etag, "Different ETags"

    print "Querying view with ?group=true"
    (resp3, view_result3) = common.query(params, viewname, {"group": "true"})
    etag3 = resp3.getheader("ETag")

    assert len(view_result3["rows"]) == expected, "Query returned % rows" % (expected,)
    assert etag3 != etag, "Different ETags"
    assert etag3 == etag2, "Equal ETags for responses 2 and 3"

    common.test_keys_sorted(view_result3)

    all_keys = {}
    for r in view_result3["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 3 was made passive" % (key,)

    print "Querying view with ?group=true&descending=true"
    (resp4, view_result4) = common.query(
        params, viewname, {"group": "true", "descending": "true"})
    etag4 = resp4.getheader("ETag")

    assert len(view_result4["rows"]) == expected, "Query returned % rows" % (expected,)
    assert etag4 != etag, "Different ETags"
    assert etag4 == etag3, "Equal ETags for responses 3 and 4"

    common.test_keys_sorted(view_result4, lambda a, b: a > b)

    all_keys = {}
    for r in view_result4["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 3 was made passive" % (key,)

    print "Querying view with ?group=true&startkey=3333&endkey=44781"
    (resp5, view_result5) = common.query(
        params, viewname,
        {"group": "true", "startkey": "3333", "endkey": "44781"})
    etag5 = resp5.getheader("ETag")

    assert etag5 != etag, "Different ETags"
    assert etag5 == etag4, "Equal ETags for responses 4 and 5"

    common.test_keys_sorted(view_result5)
    assert view_result5["rows"][0]["key"] >= 3333, "First key is >= 3333"
    assert view_result5["rows"][-1]["key"] <= 44781, "Last key is <= 44781"

    all_keys = {}
    for r in view_result5["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 3 was made passive" % (key,)

    print "Querying view with ?group=true&startkey=44781&endkey=3333&descending=true"
    (resp6, view_result6) = common.query(
        params, viewname,
        {"group": "true", "startkey": "44781", "endkey": "3333", "descending": "true"})
    etag6 = resp6.getheader("ETag")

    assert etag6 != etag, "Different ETags"
    assert etag6 == etag5, "Equal ETags for responses 5 and 6"

    common.test_keys_sorted(view_result6, lambda a, b: a > b)
    assert view_result6["rows"][0]["key"] <= 44781, "First key is <= 44781"
    assert view_result6["rows"][-1]["key"] >= 3333, "Last key is >= 3333"

    assert len(view_result6["rows"]) == len(view_result5["rows"]), \
        "Same number of rows for responses 5 and 6"

    all_keys = {}
    for r in view_result6["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 3 was made passive" % (key,)

    print "Setting partition 3 to active state"
    common.set_partition_states(params, active = [2])

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Querying view with ?group=true"
    (resp7, view_result7) = common.query(params, viewname, {"group": "true"})
    etag7 = resp7.getheader("ETag")

    assert len(view_result7["rows"]) == params["ndocs"], \
        "Query returned % rows" % (params["ndocs"],)
    assert etag7 == etag, "Same etags for responses 1 and 7"

    common.test_keys_sorted(view_result7)

    print "Querying view with ?group=true&descending=true"
    (resp8, view_result8) = common.query(
        params, viewname, {"group": "true", "descending": "true"})
    etag8 = resp8.getheader("ETag")

    assert len(view_result8["rows"]) == params["ndocs"], \
        "Query returned % rows" % (params["ndocs"],)
    assert etag7 == etag8, "Same etags for responses 7 and 8"

    common.test_keys_sorted(view_result8, lambda a, b: a > b)

    print "Querying view with ?group=true&startkey=3333&endkey=44781"
    (resp9, view_result9) = common.query(
        params, viewname,
        {"group": "true", "startkey": "3333", "endkey": "44781"})
    etag9 = resp9.getheader("ETag")

    assert etag9 == etag8, "Equal ETags for responses 8 and 9"

    common.test_keys_sorted(view_result9)
    assert view_result9["rows"][0]["key"] >= 3333, "First key is >= 3333"
    assert view_result9["rows"][-1]["key"] <= 44781, "Last key is <= 44781"

    print "Querying view with ?group=true&startkey=44781&endkey=3333&descending=true"
    (resp10, view_result10) = common.query(
        params, viewname,
        {"group": "true", "startkey": "44781", "endkey": "3333", "descending": "true"})
    etag10 = resp10.getheader("ETag")

    assert etag10 == etag9, "Equal ETags for responses 9 and 10"

    common.test_keys_sorted(view_result10, lambda a, b: a > b)
    assert view_result10["rows"][0]["key"] <= 44781, "First key is <= 44781"
    assert view_result10["rows"][-1]["key"] >= 3333, "Last key is >= 3333"

    assert len(view_result10["rows"]) == len(view_result9["rows"]), \
        "Same number of rows for responses 9 and 10"



def test_view_updates(params):
    print "Setting partition 2 state to passive"
    common.set_partition_states(params, passive = [1])

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [1], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Adding 2 new documents to partition 2"
    server = params["server"]
    db2 = server[params["setname"] + "/1"]
    new_doc1 = {"_id": "999999999", "integer": 999999999, "string": "999999999"}
    new_doc2 = {"_id": "000", "integer": -1111, "string": "000"}
    db2.save(new_doc1)
    db2.save(new_doc2)

    print "Querying map view"
    (resp, view_result) = common.query(params, "mapview1")
    etag = resp.getheader("ETag")

    expected = params["ndocs"] - (params["ndocs"] / 4)

    assert len(view_result["rows"]) == expected, \
        "Query returned %d rows" % (expected,)

    common.test_keys_sorted(view_result)

    all_keys = {}
    for r in view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(2, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 2 was made passive" % (key,)
    assert not(new_doc1["integer"] in all_keys), "new_doc1 not reflected in view"
    assert not(new_doc2["integer"] in all_keys), "new_doc2 not reflected in view"

    print "Setting partition 2 state to active"
    common.set_partition_states(params, active = [1])

    print "Querying map view again"
    (resp2, view_result2) = common.query(params, "mapview1")
    etag2 = resp2.getheader("ETag")

    expected = params["ndocs"] + 2

    assert view_result2["total_rows"] == expected, \
        "Query returned %d total_rows" % (expected,)
    assert len(view_result2["rows"]) == expected, \
        "Query returned %d rows" % (expected,)

    common.test_keys_sorted(view_result2)

    all_keys = {}
    for r in view_result2["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(2, params["ndocs"], params["nparts"]):
        assert (key in all_keys), \
            "Key %d in result after partition 2 was re-enabled" % (key,)
    assert (new_doc1["integer"] in all_keys), "new_doc1 reflected in view"
    assert (new_doc2["integer"] in all_keys), "new_doc2 reflected in view"

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        if i == 1:
            seq = (params["ndocs"] / 4) + 2
        else:
            seq = (params["ndocs"] / 4)
        assert info["update_seqs"][str(i)] == seq, "right update seq for partition %d" % (i + 1)




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

    print "Testing with map views"
    test_maps(params)
    print "Testing with reduce view redview1"
    test_reduces(params, "redview1")
    print "Testing with reduce view redview2"
    test_reduces(params, "redview2")
    print "Test view updates"
    test_view_updates(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
