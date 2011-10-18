#!/usr/bin/python

import sys
sys.path.append("../lib")
sys.path.append("common")
import json
import couchdb
import httplib
import urllib
import time
import common

HOST = "localhost:5984"
SET_NAME = "test_suite_set_view"
NUM_PARTS = 4
NUM_DOCS = 400000
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

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)

    print "Triggering partition 4 cleanup"
    common.cleanup_partition(params, 3)

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [3], "right cleanup partitions list"
    for i in [0, 1, 2]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)
    assert not("3" in info["update_seqs"]), "partition 3 not in info.update_seqs"
    assert info["cleanup_running"] == True, "cleanup process is running"

    print "Querying view again"
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

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)
    assert not("3" in info["update_seqs"]), "partition 3 not in info.update_seqs"

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

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)
    assert not("3" in info["update_seqs"]), "partition 3 not in info.update_seqs"

    print "Triggering compaction again and verifying it doesn't crash"
    common.compact_set_view(params)
    (resp5, view_result5) = common.query(params, "mapview1")
    etag5 = resp5.getheader("ETag")

    assert etag5 == etag4, "Same etag after second compaction"
    assert view_result5 == view_result4, "Same query results after second compaction"

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "right update seq for partition %d" % (i + 1)
    assert not("3" in info["update_seqs"]), "partition 3 not in info.update_seqs"



def test_set_cleanup_partitions_when_updater_is_running(params):
    print "Marking all partitions for cleanup"
    for i in [0, 1, 2, 3]:
        common.cleanup_partition(params, i)

    print "Compacting the set view group"
    common.compact_set_view(params)

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Querying view"
    (resp, view_result) = common.query(params, "mapview1")
    assert view_result["total_rows"] == 0, "Empty view result"
    assert len(view_result["rows"]) == 0, "Empty view result"

    print "Marking all partitions as active"
    for i in [0, 1, 2, 3]:
        common.enable_partition(params, i)

    print "Querying view with ?stale=update_after"
    (resp, view_result) = common.query(params, "mapview1", {"stale": "update_after"})
    assert view_result["total_rows"] == 0, "Empty view result"
    assert len(view_result["rows"]) == 0, "Empty view result"

    print "Marking partition 2 for cleanup while the updater is running"
    common.cleanup_partition(params, 1)

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [1], "right cleanup partitions list"
    assert not("1" in info["update_seqs"]), "partition 1 not in info.update_seqs"
    assert not("1" in info["purge_seqs"]), "partition 1 not in info.update_seqs"

    print "Waiting for the set view updater to finish"
    iterations = 0
    while True:
        info = common.get_set_view_info(params)
        if info["updater_running"]:
            iterations += 1
        else:
            break

    assert iterations > 0, "Updater was running when partition 2 was marked for cleanup"
    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [1] or info["cleanup_partitions"] == [], \
        "cleanup partitions list is not wrong"
    assert not("1" in info["update_seqs"]), "partition 1 not in info.update_seqs"
    assert not("1" in info["purge_seqs"]), "partition 1 not in info.update_seqs"

    print "Querying view"
    (resp, view_result) = common.query(params, "mapview1")

    doc_count = common.set_doc_count(params, [0, 2, 3])
    assert len(view_result["rows"]) == doc_count, "Query returned %d rows" % doc_count
    common.test_keys_sorted(view_result)

    all_keys = {}
    for r in view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(2, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 2 marked for cleanup" % (key,)

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    assert not("1" in info["update_seqs"]), "partition 1 not in info.update_seqs"
    assert not("1" in info["purge_seqs"]), "partition 1 not in info.update_seqs"



def test_change_partition_states_while_cleanup_running(params):
    print "Marking all partitions as active"
    common.enable_partition(params, [0, 1, 2, 3])

    doc_count = common.set_doc_count(params, [0, 1, 2, 3])
    print "Updating view"
    (resp, view_result) = common.query(params, "mapview1", {"limit": "100"})

    assert view_result["total_rows"] == doc_count, "Query returned %d total_rows" % doc_count
    assert len(view_result["rows"]) == 100, "Query returned 100 rows"
    common.test_keys_sorted(view_result)

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        expected = common.set_doc_count(params, [i])
        assert info["update_seqs"][str(i)] == expected, \
            "right update seq for partition %d" % (i + 1)

    print "Marking partitions 1 and 2 for cleanup"
    common.cleanup_partition(params, [0, 1])

    info = common.get_set_view_info(params)
    assert info["cleanup_running"] == True, "cleanup is running"
    assert info["active_partitions"] == [2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [0, 1], "right cleanup partitions list"

    print "Marking partitions 1 and 2 as active while cleanup is ongoing"
    common.enable_partition(params, [0, 1])

    print "Querying view"
    (resp, view_result) = common.query(params, "mapview1")
    doc_count = common.set_doc_count(params, [0, 1, 2, 3])

    info = common.get_set_view_info(params)
    assert view_result["total_rows"] == doc_count, "Query returned %d total_rows" % doc_count
    assert len(view_result["rows"]) == doc_count, "Query returned %d rows" % doc_count
    common.test_keys_sorted(view_result)

    all_keys = {}
    for r in view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(1, params["ndocs"], params["nparts"]):
        assert (key in all_keys), \
            "Key %d in result after partition 1 activated" % (key,)
    for key in xrange(2, params["ndocs"], params["nparts"]):
        assert (key in all_keys), \
            "Key %d in result after partition 2 activated" % (key,)
    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert (key in all_keys), \
            "Key %d (partition 3) in result set" % (key,)
    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert (key in all_keys), \
            "Key %d (partition 4) in result set" % (key,)

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        expected = common.set_doc_count(params, [i])
        assert info["update_seqs"][str(i)] == expected, \
            "right update seq for partition %d" % (i + 1)



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
    test_set_cleanup_partitions_when_updater_is_running(params)
    test_change_partition_states_while_cleanup_running(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
