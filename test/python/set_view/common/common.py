import json
import couchdb
import httplib
import urllib


def create_dbs(params, del_only = False):
    server = params["server"]
    for i in range(0, params["nparts"]):
        name = params["setname"] + "/" + str(i)
        try:
            server.delete(name)
        except:
            pass
        if not del_only:
            server.create(name)
    try:
        server.delete(params["setname"] + "/master")
    except:
        pass
    if not del_only:
        server.create(params["setname"] + "/master")


def populate(params, make_doc = lambda i: {"_id": str(i), "integer": i, "string": str(i)}):
    server = params["server"]
    dbs = []
    for i in xrange(0, params["nparts"]):
        dbs.append(server[params["setname"] + "/" + str(i)])

    master_db = server[params["setname"] + "/master"]
    master_db.save(params["ddoc"])

    for i in xrange(len(dbs)):
        docs = []
        for j in xrange(i + 1, params["ndocs"] + 1, params["nparts"]):
            docs.append(make_doc(j))

        conn = httplib.HTTPConnection(params["host"])
        conn.request(
            "POST",
            "/" + urllib.quote_plus(dbs[i].name) + "/_bulk_docs",
            json.dumps({"docs": docs}),
            {'Content-Type': 'application/json'}
            )
        resp = conn.getresponse()

        assert resp.status == 201, "_bulk_docs response had status 201"
        results = json.loads(resp.read())
        conn.close()

        for r in results:
            assert r["ok"], "Document %s created" % (r["id"])


def query(params, viewname, query_params = {}):
    qs = ""
    for (k, v) in query_params.items():
        qs = qs + "&" + k + "=" + urllib.quote_plus(v)
    if len(qs) > 0:
        qs = "?" + qs[1:]

    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "GET",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"]  + "/_view/" + viewname + qs
        )
    resp = conn.getresponse()
    assert resp.status == 200, "View query response has status 200"
    body = json.loads(resp.read())
    conn.close()
    return (resp, body)


def test_keys_sorted(view_result, comp = lambda a, b: a < b):
    for i in xrange(0, len(view_result["rows"]) - 1):
        row_a = view_result["rows"][i]
        row_b = view_result["rows"][i + 1]

        assert comp(row_a["key"], row_b["key"]), \
            "row[i].key (%s) <= row[i + 1].key (%s)" % \
            (json.dumps(row_a["key"]), json.dumps(row_b["key"]))


def define_set_view(params, active_partitions, passive_partitions):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "POST",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"] + "/_define",
        body = json.dumps(
            {
                "number_partitions": params["nparts"],
                "active_partitions": active_partitions,
                "passive_partitions": passive_partitions
                }),
        headers = {"Content-Type": "application/json"}
        )
    resp = conn.getresponse()
    assert resp.status == 201, "Set view define response has status 201"
    body = json.loads(resp.read())
    conn.close()


def disable_partition(params, i):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "POST",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"] + "/_passive_partitions",
        json.dumps([i]),
        {"Content-Type": "application/json"}
        )
    resp = conn.getresponse()
    assert resp.status == 201, "Disable partition response has status 201"
    body = json.loads(resp.read())
    conn.close()


def enable_partition(params, i):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "POST",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"] + "/_active_partitions",
        json.dumps([i]),
        {"Content-Type": "application/json"}
        )
    resp = conn.getresponse()
    assert resp.status == 201, "Enable partition response has status 201"
    body = json.loads(resp.read())
    conn.close()

def cleanup_partition(params, i):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "POST",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"] + "/_cleanup_partitions",
        json.dumps([i]),
        {"Content-Type": "application/json"}
        )
    resp = conn.getresponse()
    assert resp.status == 201, "Cleanup response has status 201"
    body = json.loads(resp.read())
    conn.close()


def compact_set_view(params, block = True):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "POST",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"] + "/_compact",
        headers = {"Content-Type": "application/json"}
        )
    resp = conn.getresponse()
    assert resp.status == 202, "Set view compact response has status 202"
    resp.read()

    if not block:
        conn.close()
        return

    while True:
        conn.request("GET", "/_active_tasks")
        resp = conn.getresponse()
        assert resp.status == 200, "Active tasks response has status 200"

        tasks = json.loads(resp.read())
        task = None
        for t in tasks:
            if t["type"] == "view_compaction" and t["set"] == params["setname"]:
                task = t
        if task is None:
            break

    conn.close()


def wait_set_view_compaction_complete(params):
    conn = httplib.HTTPConnection(params["host"])
    count = 0
    while True:
        conn.request("GET", "/_active_tasks")
        resp = conn.getresponse()
        assert resp.status == 200, "Active tasks response has status 200"

        tasks = json.loads(resp.read())
        task = None
        for t in tasks:
            if t["type"] == "view_compaction" and t["set"] == params["setname"]:
                task = t
        if task is None:
            break
        else:
            count += 1

    return count
