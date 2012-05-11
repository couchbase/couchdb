#!/usr/bin/python

try: import simplejson as json
except ImportError: import json
import couchdb
import httplib
import urllib
import common
import unittest
import time


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view"
NUM_PARTS = 8
NUM_DOCS = 400000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        },
        "redview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }",
            "reduce": "_count"
        }
    }
}


class TestReplicaIndex(unittest.TestCase):

    def setUp(self):
        self._params = {
            "host": HOST,
            "ddoc": DDOC,
            "nparts": NUM_PARTS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME,
            "server": couchdb.Server(url = "http://" + HOST)
            }
        # print "Creating databases"
        common.create_dbs(self._params)
        common.populate(self._params)
        # print "Databases created"
        # print "Configuring set view with:"
        # print "\tmaximum of 8 partitions"
        # print "\tactive partitions = [0, 1, 2, 3]"
        # print "\tpassive partitions = []"
        # print "\treplica index = True"
        active_parts = [0, 1, 2, 3]
        common.define_set_view(self._params, active_parts, [], True)


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    def test_replica_index(self):
        # print "Querying map view in steady state"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])

        self.assertEqual(view_result["total_rows"], expected_row_count,
                         "Query returned %d total_rows" % expected_row_count)
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        # print "verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [],
                         "main group has [] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(sorted(info["update_seqs"].keys()), ["0", "1", "2", "3"],
                         "right keys in update_seqs of the main index")
        for i in [0, 1, 2, 3]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))
        self.assertEqual(type(info["replica_group_info"]), dict, "group has replica_group_info")
        self.assertEqual(info["replica_group_info"]["max_number_partitions"], 8,
                          "replica group defined with maximum of 8 partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [],
                          "replica group has no passive partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")
        self.assertEqual(info["replica_group_info"]["update_seqs"].keys(), [],
                          "replica group update_seqs is empty")
        self.assertEqual(info["replica_group_info"]["stats"]["partial_updates"], 0,
                          "replica group has 0 partial updates")
        self.assertEqual(info["replica_group_info"]["stats"]["full_updates"], 0,
                          "replica group has 0 full updates")

        # print "defining partitions [4, 5, 6, 7] as replicas"
        common.add_replica_partitions(self._params, [4, 5, 6, 7])

        # print "verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 5, 6, 7],
                          "replica group has [4, 5, 6, 7] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "waiting for replica index to update"
        seconds = 0
        while info["replica_group_info"]["stats"]["full_updates"] < 1:
            time.sleep(5)
            seconds += 5
            if seconds > 900:
                raise(Exception("timeout waiting for replica index full update"))
            info = common.get_set_view_info(self._params)

        self.assertEqual(sorted(info["replica_group_info"]["update_seqs"].keys()), ["4", "5", "6", "7"],
                          "replica group update_seqs has keys [4, 5, 6, 7]")
        for i in [4, 5, 6, 7]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["replica_group_info"]["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for replica partition %d" % (expected_seq, i + 1))

        # print "Querying map view after marking partitions [4, 5, 6, 7] as replicas (not yet active)"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Querying reduce view, with ?group=false, after marking partitions [4, 5, 6, 7] as replicas (not yet active)"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview")

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after marking partitions [4, 5, 6, 7] as replicas (not yet active)"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        new_info = common.get_set_view_info(self._params)
        common.restart_server(self._params)

        # print "Querying map view again after immediately after restarting the server"
        (resp, view_result_after) = common.query(self._params, "mapview")
        self.assertEqual(view_result_after["rows"], view_result["rows"],
                         "Same view rows after server restart")

        # print "Querying reduce view, with ?group=false, after restarting the server"
        (resp, red_view_result_nogroup_after) = common.query(self._params, "redview")
        self.assertEqual(red_view_result_nogroup_after["rows"],
                         red_view_result_nogroup["rows"],
                         "Same view result as before")

        # print "Querying reduce view, with ?group=true, after restarting the server"
        (resp, red_view_result_group_after) = common.query(self._params, "redview", {"group": "true"})
        self.assertEqual(red_view_result_group_after["rows"],
                         red_view_result_group["rows"],
                         "Same view result as before")

        # print "removing partitions [5, 7] from replica set"
        common.remove_replica_partitions(self._params, [5, 7])

        # print "verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 6],
                         "main group has [4, 6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 6],
                          "replica group has [4, 6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [5, 7],
                          "replica group has [5, 7] as cleanup partitions")

        # print "waiting for replica index to finish cleanup"
        seconds = 0
        while info["replica_group_info"]["stats"]["cleanups"] < 1:
            time.sleep(5)
            seconds += 5
            if seconds > 900:
                raise(Exception("timeout waiting for replica index cleanup"))
            info = common.get_set_view_info(self._params)

        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 6],
                          "replica group has [4, 6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has [] as cleanup partitions")
        self.assertEqual(sorted(info["replica_group_info"]["update_seqs"].keys()), ["4", "6"],
                          "replica group update_seqs has keys [4, 6")

        new_info = common.get_set_view_info(self._params)
        self.restart_compare_info(info, new_info)

        info = common.get_set_view_info(self._params)
        updates_before = info["replica_group_info"]["stats"]["full_updates"]

        # print "defining partitions [5, 7] as replicas again"
        common.add_replica_partitions(self._params, [5, 7])

        # replica group likely has [5, 7] in a pending transition, just wait a while if so
        self.wait_for_pending_transition_applied()

        # print "verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 5, 6, 7],
                          "replica group has [4, 5, 6, 7] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "waiting for replica index to index partitions 5 and 7"
        seconds = 0
        while info["replica_group_info"]["stats"]["full_updates"] < (updates_before + 1):
            time.sleep(5)
            seconds += 5
            if seconds > 900:
                raise(Exception("timeout waiting for replica index full update"))
            info = common.get_set_view_info(self._params)

        self.assertEqual(sorted(info["replica_group_info"]["update_seqs"].keys()), ["4", "5", "6", "7"],
                          "replica group update_seqs has keys [4, 5, 6, 7]")
        for i in [4, 5, 6, 7]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["replica_group_info"]["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for replica partition %d" % (expected_seq, i + 1))

        # print "Querying map view after marking partitions [5, 7] as replicas again (not yet active)"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Querying reduce view, with ?group=false, after marking partitions [5, 7] as replicas again (not yet active)"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview")

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after marking partitions [5, 7] as replicas again (not yet active)"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Marking partitions [4, 5, 7] as active (to be transferred from replica to main index)"
        common.set_partition_states(self._params, active = [4, 5, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [4, 5, 7], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [4, 5, 7],
                         "main group has [4, 5, 7] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [4, 5, 7],
                          "replica group has [4, 5, 7] as active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "Querying map view with ?stale=update_after to trigger transferral of" + \
        #     " replica partitions [4, 5, 7] from replica index to main index"

        updates_before = info["stats"]["full_updates"]

        (resp, view_result) = common.query(self._params, "mapview", {"stale": "update_after"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(7, common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7]), self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")

        # print "Querying reduce view, with ?group=false, after triggering transfer of replica partitions [4, 5, 7]"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after triggering transfer of replica partitions [4, 5, 7]"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")

        # print "Waiting for transferral of replica partitions [4, 5, 7] into main index"
        info = common.get_set_view_info(self._params)
        seconds = 0
        while info["stats"]["full_updates"] < (updates_before + 1):
            time.sleep(5)
            seconds += 5
            if seconds > 900:
                raise(Exception("timeout waiting for transfer of replica partitions [4, 5, 7]"))
            info = common.get_set_view_info(self._params)

        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 4, 5, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6], "main group has [6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [], "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has [] as active partitions")
        self.assertEqual(sorted(info["update_seqs"].keys()), ["0", "1", "2", "3", "4", "5", "7"],
                         "Right list of update seqs for main group")
        self.assertEqual(sorted(info["replica_group_info"]["update_seqs"].keys()), ["6"],
                         "Right list of update seqs for replica group")
        for i in [0, 1, 2, 3, 4, 5, 7]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d in main group" % (expected_seq, i + 1))
        for i in [6]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["replica_group_info"]["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d in replica group" % (expected_seq, i + 1))

        # print "Querying map view after replica partitions were transferred"
        (resp, view_result_after) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(view_result_after["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        self.assertEqual(len(view_result_after["rows"]), len(view_result["rows"]),
                         "Same number of view rows after replica partitions were transferred")
        for i in xrange(len(view_result_after["rows"])):
            self.assertEqual(view_result_after["rows"][i], view_result["rows"][i],
                             "Row %d has same content after replica partitions were transferred" % (i + 1))

        # print "Querying reduce view, with ?group=false, after replica partitions were transferred"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after replica partitions were transferred"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")

        # print "Marking partitions [4, 5, 7] for cleanup in the main group"
        common.set_partition_states(self._params, cleanup = [4, 5, 7])

        # print "Waiting for cleanup (compaction) to finish"
        common.compact_set_view(self._params)

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6], "main group has [6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [], "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has [] active partitions")

        # print "Querying map view after marking partitions [4, 5, 7] for cleanup in the main group"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Querying reduce view, with ?group=false, after marking partitions [4, 5, 7] for cleanup in the main group"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after marking partitions [4, 5, 7] for cleanup in the main group"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "defining partitions [4, 5, 7] as replicas again"
        common.add_replica_partitions(self._params, [4, 5, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 5, 6, 7],
                          "replica group has [4, 5, 6, 7] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "removing partitions [4, 5, 7] from replica set again"
        common.remove_replica_partitions(self._params, [4, 5, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6], "main group has [6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [], "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [4, 5, 7],
                          "replica group has [4, 5, 7] as cleanup partitions")

        # print "defining partitions [4, 5, 7] as replicas again"
        common.add_replica_partitions(self._params, [4, 5, 7])
        self.wait_for_pending_transition_applied()

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 5, 6, 7],
                          "replica group has [4, 5, 6, 7] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "Querying map view after marking partitions [4, 5, 7] as replicas (not active)"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Querying reduce view, with ?group=false, after marking partitions [4, 5, 7] as replicas (not active)"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after marking partitions [4, 5, 7] as replicas (not active)"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Marking partitions [4, 5, 7] as active (to be transferred from replica to main index)"
        common.set_partition_states(self._params, active = [4, 5, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [4, 5, 7], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [4, 5, 7],
                         "main group has [4, 5, 7] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [4, 5, 7],
                          "replica group has [4, 5, 7] as active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "Querying view with ?stale=update_after to trigger transferral of" + \
        #    " replica partitions [4, 5, 7] from replica index to main index"

        updates_before = info["stats"]["full_updates"]

        (resp, view_result) = common.query(self._params, "mapview", {"stale": "update_after", "limit": "1"})

        # print "unmarking partitions [4, 5] as replicas while partitions [4, 5, 7] are being transferred from replica to main group"
        common.remove_replica_partitions(self._params, [4, 5])

        # print "Waiting for transferral of replica partitions [7] into main index"
        info = common.get_set_view_info(self._params)
        seconds = 0
        while info["stats"]["full_updates"] < (updates_before + 1):
            time.sleep(5)
            seconds += 5
            if seconds > 900:
                raise(Exception("timeout waiting for transfer of replica partitions [7]"))
            info = common.get_set_view_info(self._params)

        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6], "main group has [6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [], "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has [] as active partitions")
        self.assertEqual(sorted(info["update_seqs"].keys()), ["0", "1", "2", "3", "7"],
                         "Right list of update seqs for main group")
        self.assertEqual(sorted(info["replica_group_info"]["update_seqs"].keys()), ["6"],
                         "Right list of update seqs for replica group")
        for i in [0, 1, 2, 3, 7]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d in main group" % (expected_seq, i + 1))
        for i in [6]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["replica_group_info"]["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d in replica group" % (expected_seq, i + 1))

        # print "Querying map view after removing replica partitions [4, 5]"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 7])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 7 in view result")

        # print "Querying reduce view, with ?group=false, after removing replica partitions [4, 5]"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3, 7])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after removing replica partitions [4, 5]"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 7])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 7 in view result")

        # print "Marking partitions [7] for cleanup in the main group"
        common.set_partition_states(self._params, cleanup = [7])

        # print "Waiting for cleanup (compaction) to finish"
        common.compact_set_view(self._params)

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6], "main group has [6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [], "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has [] active partitions")

        # print "Querying map view after removing replica partitions [7]"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Querying reduce view, with ?group=false, after removing replica partitions [7]"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after removing replica partitions [7]"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "defining partitions [4, 5, 7] as replicas again"
        common.add_replica_partitions(self._params, [4, 5, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [],
                         "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [4, 5, 6, 7],
                          "replica group has [4, 5, 6, 7] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has no active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "Querying map view after marking partitions [4, 5, 7] as replicas (not yet active)"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Querying reduce view, with ?group=false, after marking partitions [4, 5, 7] as replicas (not yet active)"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after marking partitions [4, 5, 7] as replicas (not yet active)"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 7 not in view result")

        # print "Marking partitions [4, 5, 7] as active (to be transferred from replica to main index)"
        common.set_partition_states(self._params, active = [4, 5, 7])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [4, 5, 7], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [4, 5, 6, 7],
                         "main group has [4, 5, 6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [4, 5, 7],
                         "main group has [4, 5, 7] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [4, 5, 7],
                          "replica group has [4, 5, 7] as active partitions")
        self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [],
                          "replica group has no cleanup partitions")

        # print "Querying view with ?stale=update_after to trigger transferral of" + \
        #     " replica partitions [4, 5, 7] from replica index to main index"

        updates_before = info["stats"]["full_updates"]

        (resp, view_result) = common.query(self._params, "mapview", {"stale": "update_after", "limit": "1"})

        # print "marking partitions [4, 5] for cleanup while partitions [4, 5, 7] are being transferred from replica to main group"
        common.set_partition_states(self._params, cleanup = [4, 5])

        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], [0, 1, 2, 3], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [7], "right passive partitions list")
        # Not tested due to race conditions
        # self.assertEqual(info["cleanup_partitions"], [4, 5], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6, 7],
                         "main group has [6, 7] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [7],
                         "main group has [7] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [7],
                          "replica group has [7] as active partitions")
        # Not tested due to race conditions
        # self.assertEqual(info["replica_group_info"]["cleanup_partitions"], [4, 5],
        #                  "replica group has [4, 5] as cleanup partitions")

        # print "Waiting for transferral of replica partitions [7] into main index"
        info = common.get_set_view_info(self._params)
        seconds = 0
        while info["stats"]["full_updates"] < (updates_before + 1):
            time.sleep(5)
            seconds += 5
            if seconds > 900:
                raise(Exception("timeout waiting for transfer of replica partitions [7]"))
            info = common.get_set_view_info(self._params)

        self.assertEqual(info["active_partitions"], [0, 1, 2, 3, 7], "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["replica_partitions"], [6], "main group has [6] as replica partitions")
        self.assertEqual(info["replicas_on_transfer"], [], "main group has [] as replicas on transfer")
        self.assertEqual(info["replica_group_info"]["passive_partitions"], [6],
                          "replica group has [6] as passive partitions")
        self.assertEqual(info["replica_group_info"]["active_partitions"], [],
                          "replica group has [] as active partitions")
        self.assertEqual(sorted(info["update_seqs"].keys()), ["0", "1", "2", "3", "7"],
                         "Right list of update seqs for main group")
        self.assertEqual(sorted(info["replica_group_info"]["update_seqs"].keys()), ["6"],
                         "Right list of update seqs for replica group")
        for i in [0, 1, 2, 3, 7]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d in main group" % (expected_seq, i + 1))
        for i in [6]:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["replica_group_info"]["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d in replica group" % (expected_seq, i + 1))

        # print "Querying map view after marking partitions [4, 5] for cleanup"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 7])
        self.assertEqual(len(view_result["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(view_result)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in view_result["rows"]:
            all_keys[row["key"]] = row["value"]
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 7 in view result")

        # print "Querying reduce view, with ?group=false, after marking partitions [4, 5] for cleanup"
        (resp, red_view_result_nogroup) = common.query(self._params, "redview", {"stale": "ok"})

        expected_reduce_value = common.set_doc_count(self._params, [0, 1, 2, 3, 7])
        self.assertEqual(len(red_view_result_nogroup["rows"]), 1, "Query returned 1 row")
        self.assertTrue(red_view_result_nogroup["rows"][0]["key"] is None, "Key is null")
        self.assertEqual(red_view_result_nogroup["rows"][0]["value"], expected_reduce_value,
                         "Reduce value is %d with ?group=false" % expected_reduce_value)

        # print "Querying reduce view, with ?group=true, after marking partitions [4, 5] for cleanup"
        (resp, red_view_result_group) = common.query(self._params, "redview", {"group": "true", "stale": "ok"})

        expected_row_count = common.set_doc_count(self._params, [0, 1, 2, 3, 7])
        self.assertEqual(len(red_view_result_group["rows"]), expected_row_count,
                         "Query returned %d rows" % expected_row_count)
        common.test_keys_sorted(red_view_result_group)

        doc_count = common.set_doc_count(self._params, [0, 1, 2, 3, 4, 5, 6, 7])
        all_keys = {}
        for row in red_view_result_group["rows"]:
            all_keys[row["key"]] = row["value"]
            self.assertEqual(row["value"], 1, "Value for key %d is 1" % row["key"])
        for key in xrange(1, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 0 in view result")
        for key in xrange(2, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 1 in view result")
        for key in xrange(3, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 2 in view result")
        for key in xrange(4, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 3 in view result")
        for key in xrange(5, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 4 not in view result")
        for key in xrange(6, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 5 not in view result")
        for key in xrange(7, doc_count, self._params["nparts"]):
            self.assertFalse(key in all_keys, "Key from partition 6 not in view result")
        for key in xrange(8, doc_count, self._params["nparts"]):
            self.assertTrue(key in all_keys, "Key from partition 7 in view result")


    def restart_compare_info(self, info_before, info_after):
        # print "Restarting server"
        time.sleep(1)
        common.restart_server(self._params)

        del info_before["stats"]
        del info_before["replica_group_info"]["stats"]
        del info_after["stats"]
        del info_after["replica_group_info"]["stats"]
        self.assertEqual(info_after, info_before, "same index state after server restart")


    def wait_for_pending_transition_applied(self):
        # print "Waiting for pending transition to be applied"
        iterations = 0
        while True:
            if iterations > 600:
                raise(Exception("timeout waiting for pending transition to be applied"))
            info = common.get_set_view_info(self._params)
            if (info["pending_transition"] is None) and \
                (info["replica_group_info"]["pending_transition"] is None):
                break
            else:
                time.sleep(1)
                iterations += 1
