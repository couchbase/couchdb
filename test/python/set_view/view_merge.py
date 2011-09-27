#!/usr/bin/env python

import sys
sys.path.append("../lib")
sys.path.append("common")
from copy import deepcopy

import couchdb
import httplib
import common
import json
import urllib

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

def set_spec(name, view, partitions):
    return (name, {"view" : "test/%s" % view,
                   "partitions" : partitions})

def views_spec(specs, set_specs):
    specs_dict = dict(specs)
    specs_dict["sets"] = dict(set_specs)

    return {"views" : specs_dict}

def merge_spec(server, specs, set_specs):
    return ("http://%s/_view_merge/" % server, views_spec(specs, set_specs))

def do_query(server, spec, params={}):
    url = "http://%s/_view_merge/" % server
    if params:
        url += "?%s" % urllib.urlencode(params)

    conn = httplib.HTTPConnection(server)
    conn.request("POST", url,
                 headers={"Content-Type": "application/json"},
                 body=json.dumps(spec))

    resp = conn.getresponse()
    body = json.loads(resp.read())
    conn.close()
    return (resp, body)

def query(server, spec, params={}):
    resp, body = do_query(server, spec, params)
    assert resp.status == 200, "View query response has status %d" % resp.status
    return (resp, body)

def test_mapview(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec)

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    common.test_keys_sorted(result)

def test_redview(local, remote):
    local_spec = set_spec(local["setname"], "redview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "redview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec)

    assert len(result["rows"]) == 1, "Query returned invalid number of rows"
    assert result["rows"][0]["value"] == local["ndocs"] + remote["ndocs"], \
        "Non-grouped reduce value is not %d" % (local["ndocs"] + remote["ndocs"],)

def test_include_docs(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec, params={"include_docs" : True})

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    common.test_keys_sorted(result)

    for row in result["rows"]:
        key = row["key"]
        doc = row["doc"]

        assert doc["integer"] == key and doc["string"] == str(key), \
            "Invalid `doc` returned for %d key" % key

def test_limit(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec, params={"limit" : 10})

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    common.test_keys_sorted(result)

    assert len(result["rows"]) == 10, "Invalid row count"

    for (i, row) in enumerate(result["rows"], 1):
        key = row["key"]

        assert key == i, "Got invalid key"

def test_skip(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec,
                      params={"limit" : 10, "skip" : 10})

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    common.test_keys_sorted(result)

    assert len(result["rows"]) == 10, "Invalid row count"

    for (i, row) in enumerate(result["rows"], 11):
        key = row["key"]

        assert key == i, "Got invalid key"

def test_filter(local, remote):
    local_spec = set_spec(local["setname"], "mapview", [0])
    remote_spec = set_spec(remote["setname"], "mapview", [remote["nparts"] - 1])
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec)

    common.test_keys_sorted(result)

    for row in result["rows"]:
        key = row["key"]
        if key <= local["ndocs"]:
            assert key % local["nparts"] == 1, \
                "Got a document from filtered partition"
        else:
            assert key % remote["nparts"] == 0, \
                "Got a document from filtered partition"

def test_start_end_key(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    start_key = local["ndocs"] - 10
    end_key = local["ndocs"] + 10

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec,
                      params={"start_key" : start_key,
                              "end_key" : end_key})

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    common.test_keys_sorted(result)

    for row in result["rows"]:
        key = row["key"]

        assert key >= start_key and key <= end_key, \
            "Got an item with invalid key %d" % key

def test_descending(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    _, result = query(local["host"], full_spec, params={"descending" : True})

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    common.test_keys_sorted(result, operator.gt)

def test_key(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])

    n = local["ndocs"] + remote["ndocs"]
    for i in xrange(100):
        wanted_key = random.randint(1, n)
        _, result = query(local["host"], full_spec,
                          params={"key" : wanted_key})

        assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
            "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
        assert len(result["rows"]) == 1, "Result has more than one row"

        key = result["rows"][0]["key"]
        assert key == wanted_key, \
            "Returned key %d differs from the requested %d" % (key, wanted_key)

def test_keys(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    n = local["ndocs"] + remote["ndocs"]
    wanted_keys = [random.randint(1, n) for i in xrange(100)]

    full_spec = views_spec([remote_merge], [local_spec])
    full_spec["keys"] = wanted_keys

    _, result = query(local["host"], full_spec,
                      params={"keys" : wanted_keys})

    assert result["total_rows"] == local["ndocs"] + remote["ndocs"], \
        "Total rows differs from %d" % (local["ndocs"] + remote["ndocs"],)
    keys = set(row["key"] for row in result["rows"])

    assert set(wanted_keys) == keys, \
        "Got response having excessive or missing some of the requested keys"

def test_set_view_outdated_local(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"] + 1))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"]))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    (resp, body) = do_query(local["host"], full_spec)
    assert 'error' in body and body.get('reason', 'set_view_outdated')


def test_set_view_outdated_remote(local, remote):
    local_spec = set_spec(local["setname"], "mapview", range(local["nparts"]))
    remote_spec = set_spec(remote["setname"], "mapview", range(remote["nparts"] + 1))
    remote_merge = merge_spec(remote["host"], [], [remote_spec])

    full_spec = views_spec([remote_merge], [local_spec])
    (resp, body) = do_query(local["host"], full_spec)
    assert 'error' in body and body.get('reason', 'set_view_outdated')

def test_view_merge(local, remote):
    test_mapview(local, remote)
    test_redview(local, remote)
    test_include_docs(local, remote)
    test_limit(local, remote)
    test_skip(local, remote)
    test_filter(local, remote)
    test_start_end_key(local, remote)
    test_descending(local, remote)
    test_key(local, remote)
    test_keys(local, remote)
    test_set_view_outdated_local(local, remote)

def main():
    server = couchdb.Server(url = "http://" + HOST)
    params_local = {
        "host": HOST,
        "ddoc": deepcopy(DDOC),
        "nparts": NUM_PARTS,
        "ndocs": NUM_DOCS,
        "setname": SET_NAME_LOCAL,
        "server": server
    }
    params_remote = {
        "host": HOST,
        "ddoc": deepcopy(DDOC),
        "nparts": NUM_PARTS,
        "start_id": NUM_DOCS,
        "ndocs": NUM_DOCS,
        "setname": SET_NAME_REMOTE,
        "server": server
    }

    print "Creating databases"
    common.create_dbs(params_local)
    common.create_dbs(params_remote)
    common.populate(params_local)
    common.populate(params_remote)

    common.define_set_view(params_local, range(NUM_PARTS), [])
    common.define_set_view(params_remote, range(NUM_PARTS), [])
    print "Databases created"

    test_view_merge(params_local, params_remote)

    print "Deleting test data"
    common.create_dbs(params_local, True)
    common.create_dbs(params_remote, True)
    print "Done\n"

if __name__ == '__main__':
    main()
