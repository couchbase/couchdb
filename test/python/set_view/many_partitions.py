#!/usr/bin/python

try: import simplejson as json
except ImportError: import json
import couchdb
import httplib
import urllib
import common
import unittest


HOST = "localhost:5984"
SET_NAME = "test_suite_set_view"
# Can't use something higher than 96 on Mac OS X, start getting
# system_limit errors due to too many file descriptors used
NUM_PARTS = 96
NUM_DOCS = 2000
DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        }
    }
}


class TestManyPartitions(unittest.TestCase):

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
        active_parts = range(self._params["nparts"])
        common.define_set_view(self._params, active_parts, [])


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params, True)


    def test_many_partitions(self):
        total_doc_count = common.set_doc_count(self._params, range(self._params["nparts"]))

        # print "Querying view"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(len(view_result["rows"]), total_doc_count, "number of received rows is %d" % total_doc_count)
        self.assertEqual(view_result["total_rows"], total_doc_count, "total_rows is %d" % total_doc_count)
        common.test_keys_sorted(view_result)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], range(self._params["nparts"]), "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in xrange(self._params["nparts"]):
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))

        # print "Adding 1 new document to each partition"
        new_docs = []
        for i in xrange(self._params["nparts"]):
            server = self._params["server"]
            db = self._params["server"][self._params["setname"] + "/" + str(i)]
            value = total_doc_count + i + 1
            new_doc = {
                "_id": str(value),
                "integer": value,
                "string": str(value)
                }
            new_docs.append(new_doc)
            db.save(new_doc)

        new_total_doc_count = common.set_doc_count(self._params, range(self._params["nparts"]))
        self.assertEqual(new_total_doc_count, (total_doc_count + len(new_docs)), "N documents were added")
        total_doc_count = new_total_doc_count

        # print "Querying view again"
        (resp, view_result) = common.query(self._params, "mapview")

        self.assertEqual(len(view_result["rows"]), total_doc_count, "number of received rows is %d" % total_doc_count)
        self.assertEqual(view_result["total_rows"], total_doc_count, "total_rows is %d" % total_doc_count)
        common.test_keys_sorted(view_result)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], range(self._params["nparts"]), "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in xrange(self._params["nparts"]):
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))

        # print "Marking half of the partitions as passive"
        passive = range(self._params["nparts"] / 2, self._params["nparts"])
        active = range(self._params["nparts"] / 2)
        common.set_partition_states(
            self._params,
            passive = range(self._params["nparts"] / 2, self._params["nparts"])
            )

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], active, "right active partitions list")
        self.assertEqual(info["passive_partitions"], passive, "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        for i in xrange(self._params["nparts"]):
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))

        # print "Querying view again"
        (resp, view_result) = common.query(self._params, "mapview")

        expected_row_count = common.set_doc_count(self._params, active)
        self.assertEqual(len(view_result["rows"]), expected_row_count, "number of received rows is %d" % expected_row_count)
        common.test_keys_sorted(view_result)

        for row in view_result['rows']:
            if row["key"] >= 2001:
                key_part = ((row["key"] - 2000) % self._params["nparts"]) - 1
            else:
                key_part = (row["key"] % self._params["nparts"]) - 1
            self.assertTrue(key_part in active, "Key %d from passive partition not in result set" % row["key"])

        # print "Marking half of the partitions for cleanup"
        common.set_partition_states(
            self._params,
            cleanup = passive
            )
        cleanup = passive

        common.compact_set_view(self._params)

        # print "Verifying group info"
        info = common.get_set_view_info(self._params)
        self.assertEqual(info["active_partitions"], active, "right active partitions list")
        self.assertEqual(info["passive_partitions"], [], "right passive partitions list")
        self.assertEqual(info["cleanup_partitions"], [], "right cleanup partitions list")
        self.assertEqual(info["stats"]["compactions"], 1, "1 compaction")
        self.assertEqual(info["stats"]["cleanups"], 1, "1 full cleanup")
        for i in active:
            expected_seq = common.partition_update_seq(self._params, i)
            self.assertEqual(info["update_seqs"][str(i)], expected_seq,
                             "right update seq number (%d) for partition %d" % (expected_seq, i + 1))
