#!/usr/bin/env python

import sys
sys.path.append("../lib")
sys.path.append("common")
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
            "map": "function(doc) { emit(doc._id); }",
            "reduce": "_count"
        }
    }
}


class TestViewMerge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        server = couchdb.Server(url = "http://" + HOST)
        cls._params_local = {
            "host": HOST,
            "ddoc": deepcopy(DDOC),
            "nparts": NUM_PARTS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME_LOCAL,
            "server": server
            }
        cls._params_remote = {
            "host": HOST,
            "ddoc": deepcopy(DDOC),
            "nparts": NUM_PARTS,
            "start_id": NUM_DOCS,
            "ndocs": NUM_DOCS,
            "setname": SET_NAME_REMOTE,
            "server": server
            }
        # print "Creating databases"
        common.create_dbs(cls._params_local)
        common.create_dbs(cls._params_remote)
        common.populate(cls._params_local)
        common.populate(cls._params_remote)

        common.define_set_view(cls._params_local, range(NUM_PARTS), [])
        common.define_set_view(cls._params_remote, range(NUM_PARTS), [])
        # print "Databases created"


    @classmethod
    def tearDownClass(cls):
        # print "Deleting test data"
        common.create_dbs(cls._params_local, True)
        common.create_dbs(cls._params_remote, True)


    def test_view_merge(self):
        self.do_test_mapview(self._params_local, self._params_remote)
        self.do_test_redview(self._params_local, self._params_remote)
        self.do_test_include_docs(self._params_local, self._params_remote)
        self.do_test_limit(self._params_local, self._params_remote)
        self.do_test_skip(self._params_local, self._params_remote)
        self.do_test_filter(self._params_local, self._params_remote)
        self.do_test_start_end_key(self._params_local, self._params_remote)
        self.do_test_descending(self._params_local, self._params_remote)
        self.do_test_key(self._params_local, self._params_remote)
        self.do_test_keys(self._params_local, self._params_remote)
        self.do_test_set_view_outdated_local(self._params_local, self._params_remote)


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
        common.test_keys_sorted(result)


    def do_test_redview(self, local, remote):
        local_spec = self.set_spec(local["setname"], "redview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "redview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec)

        self.assertEqual(len(result["rows"]), 1, "Query returned invalid number of rows")
        self.assertEqual(result["rows"][0]["value"], local["ndocs"] + remote["ndocs"],
                         "Non-grouped reduce value is not %d" % (local["ndocs"] + remote["ndocs"]))


    def do_test_include_docs(self, local, remote):
        local_spec = self.set_spec(local["setname"], "mapview", range(local["nparts"]))
        remote_spec = self.set_spec(remote["setname"], "mapview", range(remote["nparts"]))
        remote_merge = self.merge_spec(remote["host"], [], [remote_spec])

        full_spec = self.views_spec([remote_merge], [local_spec])
        _, result = self.query(local["host"], full_spec, params={"include_docs" : True})

        self.assertEqual(result["total_rows"], local["ndocs"] + remote["ndocs"],
                         "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"]))
        common.test_keys_sorted(result)

        for row in result["rows"]:
            key = row["key"]
            doc = row["doc"]
            self.assertEqual(doc["integer"], key, "Invalid `doc` returned for %d key" % key)
            self.assertEqual(doc["string"], str(key), "Invalid `doc` returned for %d key" % key)


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
