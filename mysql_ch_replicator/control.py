"""Tiny file-based control channel between the runner (parent process, HTTP
server) and a db_replicator child process.

The runner appends requests to ``<data_dir>/<db>/control.json``; the realtime
loop drains them (``take_requests``) and publishes ``control_status.json``.
Both files are written atomically (tmp + rename). Deliberately dependency-free:
the two processes already share the data directory and nothing else.
"""
import json
import os
import time
import uuid
from logging import getLogger

logger = getLogger(__name__)


class ControlChannel:
    REQUESTS_FILE = 'control.json'
    STATUS_FILE = 'control_status.json'

    def __init__(self, directory: str):
        self.directory = directory
        self.requests_path = os.path.join(directory, self.REQUESTS_FILE)
        self.status_path = os.path.join(directory, self.STATUS_FILE)

    @staticmethod
    def _write_atomic(path: str, data) -> None:
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f)
        os.rename(tmp, path)

    @staticmethod
    def _read_json(path: str, default):
        if not os.path.exists(path):
            return default
        try:
            with open(path) as f:
                return json.load(f)
        except (ValueError, OSError):
            return default

    # ---- producer side (runner / operator) -------------------------------
    def submit(self, op: str, **params) -> dict:
        request = {'id': uuid.uuid4().hex, 'op': op, 'time': time.time(), **params}
        requests = self._read_json(self.requests_path, [])
        requests.append(request)
        self._write_atomic(self.requests_path, requests)
        return request

    def read_status(self) -> dict:
        return self._read_json(self.status_path, {})

    # ---- consumer side (db_replicator realtime loop) ----------------------
    def take_requests(self) -> list:
        requests = self._read_json(self.requests_path, [])
        if requests:
            self._write_atomic(self.requests_path, [])
        return requests

    def write_status(self, status: dict) -> None:
        status = dict(status)
        status['updated'] = time.time()
        self._write_atomic(self.status_path, status)
