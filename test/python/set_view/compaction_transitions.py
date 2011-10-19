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
SET_NAME = "test_suite_set_view_compact"
NUM_PARTS = 8
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


def test_set_passive_during_compaction(params):
    print "Querying map view"
    (map_resp, map_view_result) = common.query(params, "mapview1")

    doc_count = common.set_doc_count(params, [0, 1, 2, 3])
    assert map_view_result["total_rows"] == doc_count, \
        "Query returned %d total_rows" % doc_count
    assert len(map_view_result["rows"]) == doc_count, \
        "Query returned %d rows" % doc_count

    common.test_keys_sorted(map_view_result)

    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        db = params["server"][params["setname"] + "/" + str(i)]
        seq = db.info()["update_seq"]
        assert info["update_seqs"][str(i)] == seq, \
            "right update seq for partition %d" % (i + 1)

    print "Triggering view compaction"
    common.compact_set_view(params, False)

    print "Marking partition 4 as passive"
    common.disable_partition(params, 3)

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Waiting for compaction to finish"
    compaction_was_running = (common.wait_set_view_compaction_complete(params) > 0)
    assert compaction_was_running, "Compaction was running when the view update was triggered"

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Querying map view again"
    (map_resp, map_view_result) = common.query(params, "mapview1")

    expected = common.set_doc_count(params, [0, 1, 2])
    assert map_view_result["total_rows"] == doc_count, \
        "Query returned %d total_rows" % doc_count
    assert len(map_view_result["rows"]) == expected, \
        "Query returned %d rows" % expected

    common.test_keys_sorted(map_view_result)

    all_keys = {}
    for r in map_view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, params["ndocs"], params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 set to passive" % (key,)

    print "Triggering view compaction"
    common.compact_set_view(params, False)

    print "Adding two new partitions, 5 and 6, as passive while compaction is running"
    common.disable_partition(params, [4, 5])

    print "Waiting for compaction to finish"
    compaction_was_running = (common.wait_set_view_compaction_complete(params) > 0)
    assert compaction_was_running, "Compaction was running when the view update was triggered"

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3, 4, 5], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3, 4, 5]:
        assert str(i) in info["update_seqs"], "%d in info.update_seqs" % i
    for i in [6, 7]:
        assert not(str(i) in info["update_seqs"]), "%d not in info.update_seqs" % i

    print "Querying map view again"
    (map_resp, map_view_result2) = common.query(params, "mapview1")
    assert map_view_result2["rows"] == map_view_result["rows"], \
        "Same result set as before"

    total_doc_count = common.set_doc_count(params, [0, 1, 2, 3, 4, 5, 6, 7])
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
    assert new_total_doc_count == (total_doc_count + 8), "8 documents added"
    assert len(new_docs) == 8, "8 documents added"

    info = common.get_set_view_info(params)
    if info["updater_running"]:
        print "Waiting for updater to finish"
        assert info["updater_state"] == "updating_passive", \
            "updater state is updating_passive"
        while True:
            info = common.get_set_view_info(params)
            if info["updater_running"]:
                assert info["updater_state"] == "updating_passive", \
                    "updater state is updating_passive"
                time.sleep(3)
            else:
                break

    expected_row_count = common.set_doc_count(params, [0, 1, 2])
    expected_total_rows = common.set_doc_count(params, [0, 1, 2, 3, 4, 5])

    print "Querying map view again"
    (map_resp, map_view_result) = common.query(params, "mapview1")

    assert len(map_view_result["rows"]) == expected_row_count, \
        "len(rows) is %d" % expected_row_count
    common.test_keys_sorted(map_view_result)

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3, 4, 5], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
#    print "final info: %s" % json.dumps(info, sort_keys=True, indent=4)
    for i in [0, 1, 2, 3, 4, 5]:
        assert str(i) in info["update_seqs"], "%d in info.update_seqs" % i
        expected_seq = common.partition_update_seq(params, i)
        assert info["update_seqs"][str(i)] == expected_seq, \
            "info.update_seqs[%d] is %d" % (i, expected_seq)



def test_set_active_during_compaction(params):
    print "Triggering view compaction"
    common.compact_set_view(params, False)

    print "Marking partitions 4, 5 and 6 as active while compaction is running"
    common.enable_partition(params, [3, 4, 5])

    print "Adding new partitions 7 and 8 with active state while compaction is running"
    common.enable_partition(params, [6, 7])

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Querying map view while compaction is running"
    (map_resp, map_view_result) = common.query(params, "mapview1", {"limit": "10"})

    assert len(map_view_result["rows"]) == 10, "Query returned 10 rows"
    common.test_keys_sorted(map_view_result)

    print "Waiting for compaction to finish"
    compaction_was_running = (common.wait_set_view_compaction_complete(params) > 0)
    assert compaction_was_running, "Compaction was running when the view update was triggered"

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Querying map view again"
    doc_count = common.set_doc_count(params, [0, 1, 2, 3, 4, 5, 6, 7])
    (map_resp, map_view_result) = common.query(params, "mapview1")

    assert map_view_result["total_rows"] == doc_count, \
        "Query returned %d total_rows" % doc_count
    assert len(map_view_result["rows"]) == doc_count, \
        "Query returned %d rows" % doc_count

    common.test_keys_sorted(map_view_result)

    print "Verifying group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2, 3, 4, 5, 6, 7], "right active partitions list"
    assert info["passive_partitions"] == [], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3, 4, 5, 6, 7]:
        assert str(i) in info["update_seqs"], "%d in info.update_seqs" % i
        expected_seq = common.partition_update_seq(params, i)
        assert info["update_seqs"][str(i)] == expected_seq, \
            "info.update_seqs[%d] is %d" % (i, expected_seq)



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
    print "Configuring set view with:"
    print "\tmaximum of 8 partitions"
    print "\tactive partitions = [0, 1, 2, 3, 4]"
    print "\tpassive partitions = []"
    common.define_set_view(params, [0, 1, 2, 3], [])

    test_set_passive_during_compaction(params)
    test_set_active_during_compaction(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
