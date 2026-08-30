"""Tests for M2 ingestion: manifest persistence, exactly-once idempotency, and
key->table routing. Uses fake tables + source so no live catalog is needed.
"""

import pandas as pd
import pyarrow as pa
import pytest

from aiops import ingest


# --- fakes ----------------------------------------------------------------- #
class _FakeSnapshot:
    def __init__(self, sid):
        self.snapshot_id = sid


class _FakeSchema:
    def __init__(self, arrow):
        self._arrow = arrow

    def as_arrow(self):
        return self._arrow


class FakeTable:
    """Records appends and hands out incrementing snapshot ids."""
    def __init__(self, arrow_schema):
        self._schema = _FakeSchema(arrow_schema)
        self.appends = []
        self._sid = 1000

    def schema(self):
        return self._schema

    def append(self, batch):
        self.appends.append(batch)
        self._sid += 1

    def refresh(self):
        pass

    def current_snapshot(self):
        return _FakeSnapshot(self._sid)


class FakeObject:
    def __init__(self, key, table, df):
        self.key = key
        self.table = table
        self._df = df

    def read(self):
        return self._df


class FakeSource:
    def __init__(self, objects):
        self._objects = objects

    def list_objects(self):
        return list(self._objects)


@pytest.fixture
def scene():
    arrow = pa.schema([("ts", pa.timestamp("us")), ("vm_id", pa.string())])
    tables = {"vm_metrics": FakeTable(arrow), "app_events": FakeTable(arrow)}
    df = pd.DataFrame({"ts": pd.to_datetime(["2026-01-01T00:00:00"]), "vm_id": ["vm-1"]})
    objects = [
        FakeObject("vm_metrics/dt=2026-01-01/hour=00/part-0.parquet", "vm_metrics", df),
        FakeObject("app_events/dt=2026-01-01/hour=00/part-0.parquet", "app_events", df),
    ]
    return tables, FakeSource(objects), df


# --- manifest -------------------------------------------------------------- #
def test_manifest_roundtrip_and_persistence():
    store = ingest.MemoryManifestStore()
    m = ingest.ProcessedManifest(store)
    assert m.count == 0
    m.add("k1"); m.add("k2"); m.save()
    # Reload from the same store sees the persisted keys.
    m2 = ingest.ProcessedManifest(store)
    assert m2.count == 2
    assert "k1" in m2 and "k2" in m2 and "k3" not in m2


def test_manifest_empty_store():
    m = ingest.ProcessedManifest(ingest.MemoryManifestStore(None))
    assert m.count == 0


# --- routing --------------------------------------------------------------- #
def test_table_for_key():
    assert ingest.table_for_key("vm_metrics/dt=x/part-0.parquet") == "vm_metrics"
    assert ingest.table_for_key("app_events/dt=x/part-0.parquet") == "app_events"
    assert ingest.table_for_key("something/else.parquet") is None


# --- idempotency ----------------------------------------------------------- #
def test_ingest_once_appends_all_then_nothing(scene):
    tables, source, _ = scene
    store = ingest.MemoryManifestStore()

    first = ingest.ingest_once(tables, ingest.ProcessedManifest(store), source)
    assert len(first) == 2
    assert {r.table for r in first} == {"vm_metrics", "app_events"}
    assert all(r.rows == 1 for r in first)
    assert len(tables["vm_metrics"].appends) == 1
    assert len(tables["app_events"].appends) == 1
    # snapshot ids surfaced
    assert all(r.snapshot_id > 1000 for r in first)

    # Reload manifest from the SAME store (simulates a fresh process) -> no re-append.
    second = ingest.ingest_once(tables, ingest.ProcessedManifest(store), source)
    assert second == []
    assert len(tables["vm_metrics"].appends) == 1   # unchanged
    assert len(tables["app_events"].appends) == 1


def test_partial_new_chunk_only_appends_new(scene):
    tables, source, df = scene
    store = ingest.MemoryManifestStore()
    ingest.ingest_once(tables, ingest.ProcessedManifest(store), source)

    # Add one brand-new chunk; only it should append on the next pass.
    source._objects.append(
        FakeObject("vm_metrics/dt=2026-01-01/hour=01/part-1.parquet", "vm_metrics", df))
    res = ingest.ingest_once(tables, ingest.ProcessedManifest(store), source)
    assert len(res) == 1
    assert res[0].key.endswith("part-1.parquet")
    assert len(tables["vm_metrics"].appends) == 2


def test_empty_chunk_marked_processed_without_append(scene):
    tables, source, _ = scene
    empty = pd.DataFrame({"ts": pd.to_datetime([]), "vm_id": []})
    src = FakeSource([FakeObject("vm_metrics/dt=x/empty.parquet", "vm_metrics", empty)])
    store = ingest.MemoryManifestStore()
    m = ingest.ProcessedManifest(store)
    res = ingest.ingest_once(tables, m, src)
    assert res == []
    assert len(tables["vm_metrics"].appends) == 0
    assert "vm_metrics/dt=x/empty.parquet" in m           # still marked done
