#!/usr/bin/env python

from copy import deepcopy

import couchdb
import httplib
import common
try: import simplejson as json
except ImportError: import json
import urllib
import unittest

import operator
import random

HOST = "localhost:5984"
SET_NAME_LOCAL = "test_suite_set_view_view_merge_local"
SET_NAME_REMOTE = "test_suite_set_view_view_merge_remote"
NUM_PARTS = 4
NUM_DOCS = 5000

DDOC = {
    "_id": "_design/test",
    "language": "javascript",
    "views": {
        "mapview": {
            "map": "function(doc) { emit(doc.integer, doc.string); }"
        },
        "redview": {
            "map":  "function(doc) {" \
                "emit([doc.integer, doc.string], doc.integer);" \
                "emit([doc.integer + 1, doc.string], doc.integer + 1);" \
                "}",
            "reduce": "_sum"
        },
        "redview2": {
            "map":  "function(doc) {" \
                "emit(doc.integer, doc.string);" \
                "}",
            "reduce": "_count"
        }
    }
}


class TestViewMerge(unittest.TestCase):

    def setUp(self):
        server = couchdb.Server(url = "http://" + HOST)
        self._params_local = {
            "host": HOST,
            "ddoc": deepcopy(DDOC),
            "nparts": NUM_PARTS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME_LOCAL,
            "server": server
            }
        self._params_remote = {
            "host": HOST,
            "ddoc": deepcopy(DDOC),
            "nparts": NUM_PARTS,
            "start_id": NUM_DOCS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME_REMOTE,
            "server": server
            }
        # print "Creating databases"
        common.create_dbs(self._params_local)
        common.create_dbs(self._params_remote)
        common.populate(self._params_local)
        common.populate(self._params_remote)

        common.define_set_view(self._params_local, range(NUM_PARTS), [])
        common.define_set_view(self._params_remote, range(NUM_PARTS), [])
        # print "Databases created"


    def tearDown(self):
        # print "Deleting test data"
        common.create_dbs(self._params_local, True)
        common.create_dbs(self._params_remote, True)


    def test_view_merge(self):
        self.do_test_mapview(self._params_local, self._params_remote)
        self.do_test_redview(self._params_local, self._params_remote)
        self.do_test_redview2(self._params_local, self._params_remote)
        self.do_test_include_docs(self._params_local, self._params_remote)
        self.do_test_limit0(self._params_local, self._params_remote)
        self.do_test_limit(self._params_local, self._params_remote)
        self.do_test_skip(self._params_local, self._params_remote)
        self.do_test_filter(self._params_local, self._params_remote)
        self.do_test_start_end_key(self._params_local, self._params_remote)
        self.do_test_descending(self._params_local, self._params_remote)
        self.do_test_key(self._params_local, self._params_remote)
        self.do_test_keys(self._params_local, self._params_remote)
        self.do_test_set_view_outdated_local(self._params_local, self._params_remote)
        self.do_test_query_args(self._params_local, self._params_remote)
        self.do_test_debug_info(self._params_local, self._params_remote)


    def set_spec(self, name, view, partitions):
        return (name, {"view" : "test/%s" % view,
                       "partitions" : partitions})


    def views_spec(self, specs, set_specs):
        specs_dict = dict(specs)
        specs_dict["sets"] = dict(set_specs)

        return {"views" : specs_dict}


    def merge_spec(self, server, specs, set_specs):
        return ("http://%s/_view_merge/" % server, self.views_spec(specs, set_specs))


    def do_query(self, server, spec, params = {}):
        params = deepcopy(params)
        params.setdefault("stale", "false")

        url = "http://%s/_view_merge/" % server
        if params:
            url += "?%s" % urllib.urlencode(params)

        conn = httplib.HTTPConnection(server)
        conn.request("POST", url,
                     headers = {"Content-Type": "application/json"},
                     body = json.dumps(spec))

        resp = conn.getresponse()
        body = json.loads(resp.read())
        conn.close()
        return (resp, body)


    def query(self, server, spec, params = {}):
        resp, body = self.do_query(server, spec, params)
        self.assertEqual(resp.status, 200, "View query response has status %d" % resp.status)
        return (resp, body)


    def do_test_mapview(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec)

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        self.assertEqual(len(result["rows"]), local["ndocs"] + remote["ndocs"],
                         "len(rows) from %d" % (local["ndocs"] + remote["ndocs"]))
        common.test_keys_sorted(result)


    def do_test_redview(self, local, remote):
        local_spec = self.set_spec(local["setname"], "redview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "redview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])

        _, result = self.query(local["host"], full_spec)
        self.assertEqual(len(result["rows"]), 1,
            "Query returned invalid number of rows (a)")
        self.assertEqual(result["rows"][0]["value"], 100020000,
                         "Non-grouped reduce value is not 100020000")

        _, result = self.query(local["host"], full_spec, params={"group": True})
        self.assertEqual(len(result["rows"]), 20000,
                         "Query returned invalid number of rows (b)")

        _, result = self.query(local["host"], full_spec, params={"group_level": 1})
        self.assertEqual(len(result["rows"]), 10001,
                         "Query returned invalid number of rows (c)")


    def do_test_redview2(self, local, remote):
        local_spec = self.set_spec(local["setname"], "redview2", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "redview2", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])

        expected_row_count = common.set_doc_count(local, range(local["nparts"])) + \
            common.set_doc_count(remote, range(remote["nparts"]))
        _, result = self.query(local["host"], full_spec, {"reduce": "false"})
        self.assertEqual(len(result["rows"]), expected_row_count,
            "redview2?reduce=false query returned %d rows" % expected_row_count)

        common.test_keys_sorted(result)

        _, result = self.query(local["host"], full_spec)
        self.assertEqual(len(result["rows"]), 1, "redview2?reduce=true query returned 1 row")
        self.assertEqual(result["rows"][0]["value"], expected_row_count,
                         "Non-grouped reduce value is %d" % expected_row_count)

        _, result = self.query(local["host"], full_spec, {"group": "true"})
        self.assertEqual(len(result["rows"]), expected_row_count,
            "redview2?group=true query returned %d rows" % expected_row_count)

        for i in xrange(0, len(result["rows"]) - 1):
            row1 = result["rows"][i]
            row2 = result["rows"][i + 1]
            self.assertTrue(row1["key"] < row2["key"], "row1.key < row2.key")
            self.assertEqual(row1["value"], 1, "row1.value == 1")
            self.assertEqual(row2["value"], 1, "row2.value == 1")


    def do_test_include_docs(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec, params={"include_docs" : True})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        self.assertEqual(len(result["rows"]), local["ndocs"] + remote["ndocs"],
                         "length(rows) differs from %d" % (local["ndocs"] + remote["ndocs"]))
        self.assertFalse("errors" in result, "query result has no errors")
        common.test_keys_sorted(result)

        for row in result["rows"]:
            key = row["key"]
            doc = row["doc"]
            self.assertEqual(doc["integer"], key, "Invalid `doc` returned for %d key" % key)
            self.assertEqual(doc["string"], str(key), "Invalid `doc` returned for %d key" % key)

    def do_test_limit0(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec, params = {"limit" : 0})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))

        self.assertEqual(len(result["rows"]), 0, "Invalid row count")

    def do_test_limit(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec, params = {"limit" : 10})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        common.test_keys_sorted(result)

        self.assertEqual(len(result["rows"]), 10, "Invalid row count")

        for (i, row) in enumerate(result["rows"], 1):
            key = row["key"]
            self.assertEqual(key, i, "Got invalid key")


    def do_test_skip(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec,
                               params = {"limit" : 10, "skip" : 10})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        common.test_keys_sorted(result)

        self.assertEqual(len(result["rows"]), 10, "Invalid row count")

        for (i, row) in enumerate(result["rows"], 11):
            key = row["key"]
            self.assertEqual(key, i, "Got invalid key")


    def do_test_filter(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", [0])
        remote_spec = self.set_spec(remote["setname"], "mapview", [remote["nparts"] - 1])
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec)

        common.test_keys_sorted(result)

        for row in result["rows"]:
            key = row["key"]
            if key <= local["ndocs"]:
                self.assertEqual(key % local["nparts"], 1, "Got a document from filtered partition")
            else:
                self.assertEqual(key % remote["nparts"], 0, "Got a document from filtered partition")


    def do_test_start_end_key(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        start_key = local["ndocs"] - 10
        end_key = local["ndocs"] + 10

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(
            local["host"],
            full_spec,
            params = {"start_key" : start_key, "end_key" : end_key}
            )

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        common.test_keys_sorted(result)

        for row in result["rows"]:
            key = row["key"]
            self.assertTrue(key >= start_key and key <= end_key, "Got an item with invalid key %d" % key)


    def do_test_descending(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec, params = {"descending" : True})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        common.test_keys_sorted(result, operator.gt)


    def do_test_key(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])

        n = local["ndocs"] + remote["ndocs"]
        for i in xrange(100):
            wanted_key = random.randint(1, n)
            _, result = self.query(local["host"], full_spec, params = {"key" : wanted_key})

            self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                             "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
            self.assertEqual(len(result["rows"]), 1, "Result has more than one row")

            key = result["rows"][0]["key"]
            self.assertEqual(key, wanted_key,
                             "Returned key %d differs from the requested %d" % (key, wanted_key))


    def do_test_keys(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        n = local["ndocs"] + remote["ndocs"]
        wanted_keys = [random.randint(1, n) for i in xrange(100)]

        full_spec = self.views_spec([remote_merge], [local_spec])
        full_spec["keys"] = wanted_keys

        _, result = self.query(local["host"], full_spec, params = {"keys" : wanted_keys})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        keys = set(row["key"] for row in result["rows"])

        self.assertEqual(set(wanted_keys), keys,
                         "Got response having excessive or missing some of the requested keys")


    def do_test_set_view_outdated_local(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"] + 1))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        (resp, body) = self.do_query(local["host"], full_spec)
        self.assertTrue('error' in body and body.get('reason', 'set_view_outdated'))


    def do_test_set_view_outdated_remote(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"] + 1))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        (resp, body) = self.do_query(local["host"], full_spec)
        self.assertTrue('error' in body and body.get('reason', 'set_view_outdated'))


    def do_test_query_args(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        resp, result = self.do_query(local["host"], full_spec,
                                     {"connection_timeout": '"30000"'})

        self.assertEqual(resp.status, 400, "Return status is correct")
        self.assertEqual(result["error"], "bad_request", "Correct error")
        self.assertNotEqual(result["reason"].find("connection_timeout"), -1,
                            "Correct messsage")

        resp, result = self.do_query(local["host"], full_spec,
                                     {"on_error": '"foo"'})

        self.assertEqual(resp.status, 400, "Return status is correct")
        self.assertEqual(result["error"], "bad_request", "Correct error")
        self.assertNotEqual(result["reason"].find("on_error"), -1,
                            "Correct messsage")


    def do_test_debug_info(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec, {"debug": "true"})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        self.assertEqual(len(result["rows"]), local["ndocs"] + remote["ndocs"],
                         "len(rows) from %d" % (local["ndocs"] + remote["ndocs"]))

        self.assertTrue("debug_info" in result, "Got debug_info field in response")
        debug_info = result["debug_info"]
        self.assertTrue(type(debug_info) == dict, "debug_info field is an object")
        self.assertEqual(len(debug_info), 2, "debug_info field has 2 fields")

        for (key, info) in debug_info.iteritems():
            self.assertTrue(type(info) == dict, "debug_info field is an object")

        common.test_keys_sorted(result)

        next_part = 0
        for row in result["rows"]:
            part = row["partition"]
            self.assertEqual(part, next_part, "row for key %d has right partition id field" % row["key"])
            next_part = (next_part + 1) % NUM_PARTS

        local_spec = self.set_spec(local["setname"], "redview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "redview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])

        _, result = self.query(local["host"], full_spec, {"debug": "true"})
        self.assertEqual(len(result["rows"]), 1,
            "Query returned invalid number of rows (a)")
        self.assertEqual(result["rows"][0]["value"], 100020000,
                         "Non-grouped reduce value is not 100020000")
        self.assertTrue("debug_info" in result, "Got debug_info field in response")
        debug_info = result["debug_info"]
        self.assertTrue(type(debug_info) == dict, "debug_info field is an object")
        self.assertEqual(len(debug_info), 2, "debug_info field has 2 fields")

        for (key, info) in debug_info.iteritems():
            self.assertTrue(type(info) == dict, "debug_info field is an object")

        self.assertFalse("partition" in result["rows"][0], "reduce row has no partition field")
