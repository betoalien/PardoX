from __future__ import annotations
"""
pardox_server/registry.py
Database registry: table_name -> file_path (.prdx or .parquet only).
Persisted to a JSON file so it survives server restarts.
"""
import json
import os
import threading


_ALLOWED_EXTS = {'.prdx', '.parquet'}


class Registry:
    """Thread-safe mapping of logical table names to data files."""

    def __init__(self, config_dir: str):
        self._path = os.path.join(config_dir, "databases.json")
        self._lock = threading.RLock()
        self._tables: dict[str, str] = {}   # name -> abs_path
        self._load()

    # ── persistence ────────────────────────────────────────────────────────

    def _load(self):
        if os.path.exists(self._path):
            try:
                with open(self._path) as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._tables = {k: v for k, v in data.items()
                                    if os.path.splitext(v)[1].lower() in _ALLOWED_EXTS}
            except Exception:
                pass

    def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, 'w') as f:
            json.dump(self._tables, f, indent=2)

    # ── public API ─────────────────────────────────────────────────────────

    def add(self, table_name: str, file_path: str) -> str | None:
        """Register a table. Returns None on success, error string on failure."""
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in _ALLOWED_EXTS:
            return f"Unsupported file type '{ext}'. Only .prdx and .parquet are allowed."
        if not os.path.isabs(file_path):
            return "File path must be absolute."
        if not os.path.exists(file_path):
            return f"File not found: {file_path}"
        if not table_name.replace('_', '').isalnum():
            return "Table name must contain only letters, digits and underscores."
        with self._lock:
            self._tables[table_name] = file_path
            self._save()
        return None

    def remove(self, table_name: str) -> bool:
        with self._lock:
            if table_name in self._tables:
                del self._tables[table_name]
                self._save()
                return True
        return False

    def get(self, table_name: str) -> str | None:
        with self._lock:
            return self._tables.get(table_name)

    def list(self) -> list[tuple[str, str]]:
        with self._lock:
            return list(self._tables.items())

    def names(self) -> list[str]:
        with self._lock:
            return list(self._tables.keys())

    def as_dict(self) -> dict[str, str]:
        with self._lock:
            return dict(self._tables)
