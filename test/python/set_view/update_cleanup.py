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
NUM_PARTS = 8
NUM_DOCS = 800000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


# Test that the updater also does a cleanup.
def test_update_cleanup(params):
    print "Triggering initial view update"
    t0 = time.time()
    (resp, view_result) = common.query(params, "mapview", {"limit": "1"})
    print "Update took %.2f seconds" % (time.time() - t0)

    print "Verifying group info"

    info = common.get_set_view_info(params)
    stats = info["stats"]

    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    assert stats["updates"] == 1, "1 full update done so far"
    assert stats["updater_interruptions"] == 0, "no updater interruptions so far"
    assert stats["cleanups"] == 0, "0 cleanups done"
    assert stats["cleanup_interruptions"] == 0, "no cleanup interruptions so far"

    print "Adding new partitions 5, 6, 7 and 8 and marking partitions 1 and 4 for cleanup"
    common.set_partition_states(params, active = [4, 5, 6, 7], cleanup = [0, 3])

    print "Querying view (should trigger update + cleanup)"
    t0 = time.time()
    (resp, view_result) = common.query(params, "mapview")
    t1 = time.time()

    print "Verifying query results"

    expected_total = common.set_doc_count(params, [1, 2, 4, 5, 6, 7])
    assert view_result["total_rows"] == expected_total, "total rows is %d" % expected_total
    assert len(view_result["rows"]) == expected_total, "got %d tows" % expected_total
    common.test_keys_sorted(view_result)

    all_keys = {}
    for r in view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(1, params["ndocs"], params["nparts"]):
        assert not(key in all_keys), "Key %d (partition 1) not in query result after update+cleanup" % key
    for key in xrange(2, params["ndocs"], params["nparts"]):
        assert (key in all_keys), "Key %d (partition 2) in query result after update+cleanup" % key
    for key in xrange(3, params["ndocs"], params["nparts"]):
        assert (key in all_keys), "Key %d (partition 3) in query result after update+cleanup" % key
    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not(key in all_keys), "Key %d (partition 4) not in query result after update+cleanup" % key
    for key in xrange(5, params["ndocs"], params["nparts"]):
        assert (key in all_keys), "Key %d (partition 5) in query result after update+cleanup" % key
    for key in xrange(6, params["ndocs"], params["nparts"]):
        assert (key in all_keys), "Key %d (partition 6) in query result after update+cleanup" % key
    for key in xrange(7, params["ndocs"], params["nparts"]):
        assert (key in all_keys), "Key %d (partition 7) in query result after update+cleanup" % key
    for key in xrange(8, params["ndocs"], params["nparts"]):
        assert (key in all_keys), "Key %d (partition 8) in query result after update+cleanup" % key

    print "Verifying group info"

    info = common.get_set_view_info(params)
    stats = info["stats"]

    assert info["active_partitions"] == [1, 2, 4, 5, 6, 7], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    assert stats["updates"] == 2, "2 full updates done so far"
    assert stats["updater_interruptions"] == 0, "no updater interruptions so far"
    assert stats["cleanups"] == 1, "1 full cleanup done"
    assert stats["cleanup_interruptions"] == 1, "1 cleanup interruption done"

    for i in info["active_partitions"]:
        expected_seq = common.partition_update_seq(params, i)
        assert str(i) in info["update_seqs"], "partition %d in info.update_seqs" % (i + 1)
        assert info["update_seqs"][str(i)] == expected_seq, "right seq in info.update_seqs[%d]" % i

    print "Update+cleanup took %.2f seconds, cleanup took %.2f seconds" % \
        ((t1 - t0), stats["last_cleanup_duration"])



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

    print "Creating %d databases, %d documents per database" % (NUM_PARTS, NUM_DOCS / NUM_PARTS)
    common.create_dbs(params)
    common.populate(params)
    print "Databases created"
    print "Configuring set view with:"
    print "\tmaximum of 8 partitions"
    print "\tactive partitions = [0, 1, 2, 3]"
    print "\tpassive partitions = []"
    common.define_set_view(params, [0, 1, 2, 3], [])

    test_update_cleanup(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
