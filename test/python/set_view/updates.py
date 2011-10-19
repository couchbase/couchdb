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
NUM_PARTS = 8
NUM_DOCS = 400000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


# Test adding new active partitions while the updater is running.
def test_updates(params):
    total_doc_count = common.set_doc_count(params, [0, 1, 2, 3, 4, 5, 6, 7])
    print "Querying view in steady state with ?stale=update_after"
    (resp, view_result) = common.query(params, "mapview", {"stale": "update_after"})

    assert len(view_result["rows"]) == 0, "Received empty row set"
    assert view_result["total_rows"] == 0, "Received empty row set"

    print "Adding partitions 7 and 8 as active while updater is running"
    common.enable_partition(params, [6, 7])

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3, 4, 5, 6, 7], \
        "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Waiting for updater to finish"
    count = 0
    while True:
        info = common.get_set_view_info(params)
        if info["updater_running"]:
            count += 1
        else:
            break

    assert count > 0, "Updater was running when partitions 7 and 8 were added as active"

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3, 4, 5, 6, 7], \
        "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3, 4, 5]:
        expected_seq = common.partition_update_seq(params, i)
        assert info["update_seqs"][str(i)] == expected_seq, \
            "right update seq number (%d) for partition %d" % (expected_seq, i + 1)

    print "Querying view again (steady state)"
    (resp, view_result) = common.query(params, "mapview")

    assert len(view_result["rows"]) == total_doc_count, \
        "number of received rows is %d" % total_doc_count
    assert view_result["total_rows"] == total_doc_count, \
        "total_rows is %d" % total_doc_count
    common.test_keys_sorted(view_result)

    print "Adding 1 new document to each partition"
    new_docs = []
    for i in [0, 1, 2, 3, 4, 5, 6, 7]:
        server = params["server"]
        db = params["server"][params["setname"] + "/" + str(i)]
        value = total_doc_count + i + 1
        new_doc = {
            "_id": str(value),
            "integer": value,
            "string": str(value)
        }
        new_docs.append(new_doc)
        db.save(new_doc)

    new_total_doc_count = common.set_doc_count(params, [0, 1, 2, 3, 4, 5, 6, 7])
    assert new_total_doc_count == (total_doc_count + len(new_docs)), "8 documents were added"
    total_doc_count = new_total_doc_count

    print "Querying view again (steady state)"
    (resp, view_result) = common.query(params, "mapview")

    assert len(view_result["rows"]) == total_doc_count, \
        "number of received rows is %d" % total_doc_count
    assert view_result["total_rows"] == total_doc_count, \
        "total_rows is %d" % total_doc_count
    common.test_keys_sorted(view_result)

    i = len(new_docs) - 1
    j = len(view_result["rows"]) - 1
    while i >= 0:
        assert view_result["rows"][j]["key"] == new_docs[i]["integer"], \
            "new document %s in view result set" % new_docs[i]["_id"]
        assert view_result["rows"][j]["id"] == new_docs[i]["_id"], \
            "new document %s in view result set" % new_docs[i]["_id"]
        i -= 1
        j -= 1



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
    print "Databases created"
    print "Configuring set view with:"
    print "\tmaximum of 8 partitions"
    print "\tactive partitions = [0, 1, 2, 3, 4, 5]"
    print "\tpassive partitions = []"
    common.define_set_view(params, [0, 1, 2, 3, 4, 5], [])

    test_updates(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
