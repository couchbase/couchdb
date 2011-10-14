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
NUM_DOCS = 200000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


# Verify that we have both active and passive partitions, the updater
# first indexes the active partitions and then after it indexes the
# documents from the passive partitions. Clients querying with ?stale=false
# are unblocked as soon as the indexing of the active partitions is done.
# This test verifies that property.
def test_updates(params):
    print "Querying map view"
    (resp, view_result) = common.query(params, "mapview", {"limit": "20"})

    print "Grabbing group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0], "right active partitions list"
    assert info["passive_partitions"] == [1, 2, 3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    assert info["update_seqs"]["0"] == (params["ndocs"] / 4), \
        "All data from partition 1 was indexed"

    sum_passive_seqs = 0
    for i in [1, 2, 3]:
        sum_passive_seqs += info["update_seqs"][str(i)]

    assert sum_passive_seqs < (params["ndocs"] - (params["ndocs"] / 4)), \
        "Passive partitions are still being indexed"
    assert info["updater_running"] == True, "View updater still running"
    assert info["updater_state"] == "updating_passive", "View updater in state 'updating_passive'"

    print "Verifying view query response"
    assert len(view_result["rows"]) == 20, "Query returned 20 rows"

    common.test_keys_sorted(view_result)
    assert view_result["rows"][0]["key"] == 1, "First row has key 1"
    last_key_expected = range(1, params["ndocs"], params["nparts"])[19]
    assert view_result["rows"][-1]["key"] == last_key_expected, \
        "Last row has key %d" % last_key_expected

    print "Waiting for view updater to finish"
    while True:
        info = common.get_set_view_info(params)
        if not info["updater_running"]:
            break
        else:
            time.sleep(3)

    print "Grabbing group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0], "right active partitions list"
    assert info["passive_partitions"] == [1, 2, 3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"
    for i in [0, 1, 2, 3]:
        assert info["update_seqs"][str(i)] == (params["ndocs"] / 4), \
            "Right update seq for partition %d" % (i + 1)
    assert info["updater_running"] == False, "View updater not running anymore"
    assert info["updater_state"] == "not_running", "View updater not running anymore"

    print "Querying view again and validating the response"
    (resp, view_result) = common.query(params, "mapview")

    assert len(view_result["rows"]) == (params["ndocs"] / 4), \
        "Query returned %d rows" % (params["ndocs"] / 4,)

    common.test_keys_sorted(view_result)
    assert view_result["rows"][0]["key"] == 1, "First row has key 1"
    last_key_expected = range(1, params["ndocs"], params["nparts"])[-1]
    assert view_result["rows"][-1]["key"] == last_key_expected, \
        "Last row has key %d" % last_key_expected



def test_set_passive_partitions_when_updater_is_running(params):
    print "Querying map view in steady state with ?stale=update_after"
    (resp, view_result) = common.query(params, "mapview", {"stale": "update_after"})

    assert len(view_result["rows"]) == 0, "Received empty row set"
    assert view_result["total_rows"] == 0, "Received empty row set"

    print "Marking partition 4 as passive"
    common.disable_partition(params, 3)

    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3], "right passive partitions list"
    assert info["cleanup_partitions"] == [], "right cleanup partitions list"

    print "Waiting for the set view updater to finish"
    iterations = 0
    while True:
        info = common.get_set_view_info(params)
        if info["updater_running"]:
            iterations += 1
        else:
            break

    assert iterations > 0, "Updater was running when partition 4 was set to passive"
    print "Verifying set view group info"
    info = common.get_set_view_info(params)
    assert info["active_partitions"] == [0, 1, 2], "right active partitions list"
    assert info["passive_partitions"] == [3], "right passive partitions list"
    assert info["cleanup_partitions"] == [],  "right cleanup partitions list"

    print "Querying map view again"
    (resp, view_result) = common.query(params, "mapview")

    doc_count = common.set_doc_count(params)
    expected_row_count = common.set_doc_count(params, [0, 1, 2])
    assert view_result["total_rows"] == doc_count, \
        "Query returned %d total_rows" % (doc_count,)
    assert len(view_result["rows"]) == expected_row_count, \
        "Query returned %d rows" % (expected_row_count,)

    common.test_keys_sorted(view_result)

    all_keys = {}
    for r in view_result["rows"]:
        all_keys[r["key"]] = True

    for key in xrange(4, doc_count, params["nparts"]):
        assert not (key in all_keys), \
            "Key %d not in result after partition 4 was set to passive" % (key,)


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
    print "Configuring set view with 1 active partition and 3 passive partitions"
    common.define_set_view(params, [0], [1, 2, 3])

    test_updates(params)

    print "Re-creating databases"
    del params["ddoc"]["_rev"]
    common.create_dbs(params)
    common.populate(params)
    print "Configuring set view with all partitions active"
    common.define_set_view(params, [0, 1, 2, 3], [])

    test_set_passive_partitions_when_updater_is_running(params)

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
