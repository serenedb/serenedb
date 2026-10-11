import json
import os
import re
import sys
import sysconfig
import tempfile
import time

FIRST_NORMAL_OID = 16384
USER_SCHEMAS = ("public", "inventory")
OBJECT_ENDPOINTS = ("obj", "sql", "dependency", "dependent")
CLUSTER_ENDPOINTS = ("obj", "sql")
LABELS_ONLY = ("cast", "catalog", "language")
QUERY_TOOL = 1001
VIEW_DATA = 1002
POLL_KEYS = ("status", "result", "rows_affected", "colinfo", "primary_keys",
             "additional_messages", "transaction_status", "has_oids")
QUERIES = (
    "INSERT INTO customers (name, email, mood, score, created) "
    "VALUES ('Pudge', 'pudge@dota', 'happy', 12.5, '2024-01-01 00:00:00+00')",
    "SELECT id, name, email, mood, score, active FROM customers",
    "SELECT * FROM customer_orders",
    "SELECT c.name, o.placed FROM customers c LEFT JOIN orders o ON o.customer_id = c.id",
    "SELECT add_one(41) AS answer, current_schema() AS schema",
    "SELEC 1",
)


def make_app(data_dir: str):
    os.environ["PGADMIN_SERVER_MODE"] = "OFF"
    settings = {
        "DATA_DIR": data_dir,
        "LOG_FILE": os.path.join(data_dir, "pgadmin4.log"),
        "SQLITE_PATH": os.path.join(data_dir, "pgadmin4.db"),
        "SESSION_DB_PATH": os.path.join(data_dir, "sessions"),
        "STORAGE_DIR": os.path.join(data_dir, "storage"),
        "AZURE_CREDENTIAL_CACHE_DIR": os.path.join(data_dir, "azure"),
        "KERBEROS_CCACHE_DIR": os.path.join(data_dir, "kerberos"),
        "UPGRADE_CHECK_ENABLED": False,
        "MASTER_PASSWORD_REQUIRED": False,
        "CONSOLE_LOG_LEVEL": 50,
        "FILE_LOG_LEVEL": 50,
    }
    with open(os.path.join(data_dir, "config_local.py"), "w") as config:
        config.write("".join(f"{key} = {value!r}\n" for key, value in settings.items()))
    sys.path[:0] = [data_dir, os.path.join(sysconfig.get_paths()["purelib"], "pgadmin4")]
    import pgAdmin4

    app = pgAdmin4.app
    app.config["WTF_CSRF_ENABLED"] = False
    return app


def normalize(value, names):
    if isinstance(value, dict):
        return {key: "<xid>" if key == "xmin" and item is not None
                else normalize(item, names) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item, names) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value >= FIRST_NORMAL_OID:
        return "<oid>"
    if isinstance(value, str):
        for name in names:
            value = value.replace(name, "<db>")
        return re.sub(r"(?<=_)\d{5,}\b", "<oid>", value)
    return value


class Browser:
    def __init__(self, client, names):
        self.client = client
        self.names = names

    def request(self, method: str, url: str, body=None):
        response = self.client.open(url, method=method, json=body)
        payload = response.get_json(silent=True)
        failed = isinstance(payload, dict) and payload.get("success") == 0
        if response.status_code >= 400 or payload is None or failed:
            message = payload.get("errormsg") if isinstance(payload, dict) else None
            return None, {"status": response.status_code,
                          "error": normalize(message or response.get_data(as_text=True)[:200],
                                             self.names)}
        return payload, None

    def get(self, url: str):
        return self.request("GET", url)

    def outcome(self, url: str):
        payload, failure = self.get(url)
        if failure:
            return failure
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        return normalize(payload, self.names)

    def walk(self, node_type: str, ids: str, keep) -> dict | list:
        payload, failure = self.get(f"/browser/{node_type}/nodes/{ids}/")
        if failure:
            return {"nodes": failure}
        nodes = sorted(payload["data"], key=lambda node: (node["_type"], node["label"]))
        if node_type in LABELS_ONLY:
            return [normalize(node["label"], self.names) for node in nodes]
        result = {}
        for node in nodes:
            if keep and node["label"] not in keep:
                continue
            kind, oid = node["_type"], node["_id"]
            endpoints = CLUSTER_ENDPOINTS if kind in ("role", "tablespace") \
                else OBJECT_ENDPOINTS
            entry = result[f"{kind} {normalize(node['label'], self.names)}"] = {
                endpoint: self.outcome(f"/browser/{kind}/{endpoint}/{ids}/{oid}")
                for endpoint in endpoints
            }
            for endpoint in ("dependency", "dependent"):
                if isinstance(entry.get(endpoint), list):
                    entry[endpoint].sort(key=lambda item: json.dumps(item, sort_keys=True))
            children, failure = self.get(f"/browser/{kind}/children/{ids}/{oid}")
            if failure:
                entry["children"] = failure
                continue
            for child in sorted(children["data"], key=lambda child: child["_type"]):
                child_type = child["_type"].removeprefix("coll-")
                entry[f"coll {child_type}"] = self.walk(
                    child_type, f"{ids}/{oid}",
                    USER_SCHEMAS if child_type == "schema" else None)
        return result


def poll(browser, trans_id: int):
    for _ in range(600):
        payload, failure = browser.get(f"/sqleditor/poll/{trans_id}")
        if failure:
            return failure
        data = payload["data"]
        if data["status"] != "Busy":
            return normalize({key: data.get(key) for key in POLL_KEYS}, browser.names)
        time.sleep(0.05)
    return {"error": "poll timed out"}


def node_id(browser, url: str, label: str):
    payload, _ = browser.get(url)
    return next(node["_id"] for node in payload["data"] if node["label"] == label)


def query_tool(browser, gid, sid, did, dbname) -> dict:
    result = {}
    _, failure = browser.request("POST", f"/sqleditor/initialize/sqleditor/{QUERY_TOOL}/"
                                         f"{gid}/{sid}/{did}", {"dbname": dbname})
    result["initialize"] = failure
    for sql in QUERIES:
        _, failure = browser.request("POST", f"/sqleditor/query_tool/start/{QUERY_TOOL}",
                                     {"sql": sql, "explain_plan": None})
        result[sql] = failure or poll(browser, QUERY_TOOL)
    scid = node_id(browser, f"/browser/schema/nodes/{gid}/{sid}/{did}/", "public")
    tid = node_id(browser, f"/browser/table/nodes/{gid}/{sid}/{did}/{scid}/", "customers")
    _, failure = browser.request(
        "POST", f"/sqleditor/initialize/viewdata/{VIEW_DATA}/1/table/{gid}/{sid}/{did}/{tid}",
        {})
    result["view_data initialize"] = failure
    _, failure = browser.get(f"/sqleditor/view_data/start/{VIEW_DATA}")
    result["view_data customers"] = failure or poll(browser, VIEW_DATA)
    return result


def main():
    host, port, user, dbname, output = sys.argv[1:6]
    with tempfile.TemporaryDirectory() as data_dir:
        app = make_app(data_dir)
        browser = Browser(app.test_client(), [dbname])
        groups, _ = browser.get("/browser/server_group/nodes/")
        gid = groups["data"][0]["_id"]
        server, failure = browser.request("POST", f"/browser/server/obj/{gid}/", {
            "name": "target", "host": host, "port": int(port), "db": "postgres",
            "username": user, "sslmode": "prefer", "connect_now": False})
        sid = server["node"]["_id"]
        connected, failure = browser.request(
            "POST", f"/browser/server/connect/{gid}/{sid}", {"password": ""})
        result = {"connect": failure or {key: connected["data"][key]
                                         for key in ("connected", "server_type", "type")}}
        databases, _ = browser.get(f"/browser/database/nodes/{gid}/{sid}/")
        did = next(node["_id"] for node in databases["data"] if node["label"] == dbname)
        _, failure = browser.request("POST", f"/browser/database/connect/{gid}/{sid}/{did}")
        result["database_connect"] = failure
        result["roles"] = browser.walk("role", f"{gid}/{sid}", [user])
        result["tablespaces"] = browser.walk("tablespace", f"{gid}/{sid}", None)
        result["database"] = browser.walk("database", f"{gid}/{sid}", [dbname])
        result["query_tool"] = query_tool(browser, gid, sid, did, dbname)
    with open(output, "w") as out:
        json.dump(result, out, indent=1, sort_keys=True)
        out.write("\n")


if __name__ == "__main__":
    main()
