try: import simplejson as json
except ImportError: import json
import couchdb
import httplib
import urllib
from threading import Thread


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


def set_doc_count(params, partitions = None):
    if partitions is None:
        partitions = range(0, params["nparts"])
    count = 0
    for i in partitions:
        name = params["setname"] + "/" + str(i)
        db = params["server"][name]
        count += len(db)
    return count


def partition_update_seq(params, partition):
    name = params["setname"] + "/" + str(partition)
    db = params["server"][name]
    return db.info()["update_seq"]


def populate(params, make_doc = lambda i: {"_id": str(i), "integer": i, "string": str(i)}):
    start = params.get("start_id", 0)
    server = params["server"]
    dbs = []
    for i in xrange(0, params["nparts"]):
        dbs.append(server[params["setname"] + "/" + str(i)])

    master_db = server[params["setname"] + "/master"]
    master_db.save(params["ddoc"])

    def upload_docs(db_name, doc_list, conn):
        conn.request(
            "POST",
            "/" + urllib.quote_plus(db_name) + "/_bulk_docs",
            json.dumps({"docs": doc_list}),
            {'Content-Type': 'application/json'}
            )
        resp = conn.getresponse()

        assert resp.status == 201, "_bulk_docs response had status 201"
        results = json.loads(resp.read())
        assert results["ok"] == True, "Documents uploaded"

    def populate_db(db_num, db_name):
        docs = []
        conn = httplib.HTTPConnection(params["host"])
        for i in xrange(db_num, params["ndocs"] + 1, params["nparts"]):
            docs.append(make_doc(start + i))
            if len(docs) >= 1000:
                upload_docs(db_name, docs, conn)
                docs = []

        if len(docs) > 0:
            upload_docs(db_name, docs, conn)
        conn.close()

    threads = []
    for i in xrange(len(dbs)):
        t = Thread(None, populate_db, None, (i + 1, dbs[i].name))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


def delete_doc(params, db_name, id):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "DELETE",
        "/" + urllib.quote_plus(db_name) + "/" + urllib.quote_plus(id)
        )
    resp = conn.getresponse()
    assert resp.status == 200, "Doc delete response has status 200"
    body = json.loads(resp.read())
    conn.close()


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
    conn.request("GET", "/_config/couchdb/max_dbs_open")
    resp = conn.getresponse()
    assert resp.status == 200, "GET /_config/couchdb/max_dbs_open response code 200"
    max_dbs_open = int(json.loads(resp.read()))

    if params["nparts"] >= max_dbs_open:
        max_dbs_open = params["nparts"] * 2
        conn.request("PUT", "/_config/couchdb/max_dbs_open", json.dumps(str(max_dbs_open)))
        resp = conn.getresponse()
        assert resp.status == 200, "PUT /_config/couchdb/max_dbs_open response code 200"
        json.loads(resp.read())

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


def set_partition_states(params, active = [], passive = [], cleanup = []):
    body = json.dumps(
        {
            "active": active,
            "passive": passive,
            "cleanup": cleanup
        })
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "POST",
        "/_set_view/" + params["setname"] + "/" + \
            params["ddoc"]["_id"] + "/_set_partition_states",
        body,
        {"Content-Type": "application/json"}
        )
    resp = conn.getresponse()
    assert resp.status == 201, "_set_partition_states response has status 201"
    body = json.loads(resp.read())
    conn.close()


def get_set_view_info(params):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "GET",
        "/_set_view/" + params["setname"] + "/" + params["ddoc"]["_id"] + "/_info"
        )
    resp = conn.getresponse()
    assert resp.status == 200, "Set view info response has status 200"
    info = json.loads(resp.read())
    conn.close()
    return info


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
        info = get_set_view_info(params)
        if not info["compact_running"]:
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


def set_config_parameter(params, section, name, value):
    conn = httplib.HTTPConnection(params["host"])
    conn.request(
        "PUT",
        "/_config/" + section + "/" + name,
        json.dumps(str(value))
        )
    resp = conn.getresponse()
    assert resp.status == 200, "config update response code is 200"
    json.loads(resp.read())
    conn.close()

