"""Connexion Turso (API HTTP) qui imite sqlite3 pour app.py."""
import base64
import os
import re
import sqlite3
import time

import requests


class Row(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


def _arg(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, bytes):
        return {"type": "blob", "base64": base64.b64encode(v).decode()}
    return {"type": "text", "value": str(v)}


def _cell(c):
    t = c.get("type")
    if t == "null":
        return None
    if t == "integer":
        return int(c["value"])
    if t == "float":
        return float(c["value"])
    if t == "blob":
        return base64.b64decode(c.get("base64", ""))
    return c.get("value")


class Result:
    def __init__(self, rows, lastrowid=None, rowcount=0):
        self._rows = rows
        self.lastrowid = lastrowid
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class TursoConnection:
    row_factory = None

    def __init__(self, url, token=None):
        host = re.sub(r"^(?:[a-zA-Z][a-zA-Z0-9+.-]*://)+", "", url.strip().strip("'\"")).strip("/")
        self.url = f"https://{host}/v2/pipeline"
        self.token = token.strip().strip("'\"") if token else None

    def execute(self, sql, params=()):
        args = [_arg(p) for p in (params or ())]
        stmt = {"sql": sql}
        if args:
            stmt["args"] = args
        body = {"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        resp = None
        for attempt in range(3):
            try:
                resp = requests.post(self.url, json=body, headers=headers, timeout=15)
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt == 2:
                    raise
                time.sleep(0.5 * (attempt + 1))
        resp.raise_for_status()

        item = resp.json()["results"][0]
        if item["type"] == "error":
            msg = item["error"].get("message", "Erreur Turso")
            if "constraint" in msg.lower():
                raise sqlite3.IntegrityError(msg)
            raise sqlite3.OperationalError(msg)

        res = item["response"]["result"]
        names = [c.get("name") for c in res.get("cols", [])]
        rows = [Row(zip(names, [_cell(c) for c in r])) for r in res.get("rows", [])]
        last_id = res.get("last_insert_rowid")
        return Result(rows, int(last_id) if last_id is not None else None,
                      res.get("affected_row_count", 0))

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def connect():
    return TursoConnection(os.environ["TURSO_DATABASE_URL"], os.environ.get("TURSO_AUTH_TOKEN"))