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

    print "Deleting test data"
    common.create_dbs(params, True)
    print "Done\n"


main()
