"""Continuous raw-parquet -> AIStor Iceberg Tables ingestion (M2).

Append-only, one snapshot per chunk, exactly-once via a processed-manifest.

The AIStor Iceberg REST catalog needs one non-obvious workaround: its sticky
session cookie, rewritten by the OpenShift route, breaks PyIceberg's SigV4
signing of write requests. We block cookies on the catalog session; see
iceberg_catalog(). Reads work either way.

Storage-agnostic pieces (RawSource, ProcessedManifest) are separated from the
catalog so the manifest/idempotency logic is unit-testable without a live lake.
"""

from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from http.cookiejar import DefaultCookiePolicy
from pathlib import Path

import pandas as pd

from aiops.config import get_bool

# --------------------------------------------------------------------------- #
# Iceberg schema for the data contract (field ids are stable and must not move)
# --------------------------------------------------------------------------- #
NAMESPACE_DEFAULT = "observability"

# (name, iceberg-type-name) in contract order; ts is the day() partition source.
_VM_METRICS_FIELDS = [
    ("ts", "timestamp"), ("vm_id", "string"), ("app", "string"), ("site", "string"),
    ("cpu_pct", "float"), ("heap_mb", "float"), ("gc_pause_ms", "float"),
    ("disk_io_await_ms", "float"), ("disk_iops", "float"), ("net_mbps", "float"),
    ("net_retrans_pct", "float"), ("app_latency_ms", "float"), ("error_rate_pct", "float"),
]
_APP_EVENTS_FIELDS = [
    ("ts", "timestamp"), ("vm_id", "string"), ("app", "string"), ("site", "string"),
    ("level", "string"), ("event_type", "string"), ("message", "string"),
]

TABLE_FIELDS = {"vm_metrics": _VM_METRICS_FIELDS, "app_events": _APP_EVENTS_FIELDS}

# Static dimension (M5): no ts, so unpartitioned and overwritten (not appended).
TOPOLOGY_FIELDS = [("vm_id", "string"), ("app", "string"), ("site", "string"),
                   ("depends_on_app", "string")]


def iceberg_schema(fields):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (NestedField, TimestampType, StringType, FloatType)
    type_map = {"timestamp": TimestampType, "string": StringType, "float": FloatType}
    cols = [NestedField(i + 1, name, type_map[t](), required=False)
            for i, (name, t) in enumerate(fields)]
    return Schema(*cols)


def day_partition_spec():
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform
    # source_id=1 -> ts (first field in every table)
    return PartitionSpec(PartitionField(source_id=1, field_id=1000,
                                        transform=DayTransform(), name="ts_day"))


def ensure_table(catalog, namespace: str, name: str, fields,
                 partitioned: bool = True) -> object:
    """Create the table if missing (day(ts)-partitioned unless partitioned=False,
    e.g. the static topology dimension); return the loaded Table."""
    from pyiceberg.exceptions import TableAlreadyExistsError
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    ident = (namespace, name)
    spec = day_partition_spec() if partitioned else UNPARTITIONED_PARTITION_SPEC
    try:
        catalog.create_table(ident, schema=iceberg_schema(fields), partition_spec=spec)
    except TableAlreadyExistsError:
        pass
    return catalog.load_table(ident)


def load_topology(catalog, df, namespace: str = NAMESPACE_DEFAULT) -> int:
    """Overwrite the static topology table with df (it's a dimension, not a
    stream). Returns the row count written."""
    import pyarrow as pa
    table = ensure_table(catalog, namespace, "topology", TOPOLOGY_FIELDS,
                         partitioned=False)
    arrow = pa.Table.from_pandas(df[[f[0] for f in TOPOLOGY_FIELDS]],
                                 schema=table.schema().as_arrow(), preserve_index=False)
    table.overwrite(arrow)
    return len(df)


# --------------------------------------------------------------------------- #
# Catalog connection (the proven AIStor recipe)
# --------------------------------------------------------------------------- #
def iceberg_catalog(cfg, insecure: bool = False):
    """Build a RestCatalog for AIStor's /_iceberg endpoint with the cookie
    workaround applied. `cfg` is a ConfigParser from aiops.config.load_config."""
    ak = cfg.get("iceberg", "access_key")
    sk = cfg.get("iceberg", "secret_key")
    # PyIceberg's SigV4 signer resolves creds from the botocore/env chain.
    os.environ.setdefault("AWS_ACCESS_KEY_ID", ak)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", sk)
    os.environ.setdefault("AWS_REGION", "us-east-1")

    from pyiceberg.catalog.rest import RestCatalog
    catalog = RestCatalog("aistor", **{
        "uri": cfg.get("iceberg", "uri"),
        "warehouse": cfg.get("iceberg", "warehouse"),
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": "us-east-1",
        "s3.endpoint": cfg.get("iceberg", "s3_endpoint"),
        "s3.access-key-id": ak,
        "s3.secret-access-key": sk,
        "s3.region": "us-east-1",
        "s3.force-virtual-addressing": "false",   # path-style, like mc
    })
    # THE FIX: block the sticky session cookie so SigV4-signed writes verify.
    catalog._session.cookies.clear()
    catalog._session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
    if insecure:
        import urllib3
        urllib3.disable_warnings()
        catalog._session.verify = False
    return catalog


def minio_client(cfg, insecure: bool = False):
    """Build a MinIO client + raw bucket name from [minio] config."""
    from minio import Minio
    secure = get_bool(cfg, "minio", "secure", fallback=True)
    kwargs = dict(access_key=cfg.get("minio", "access_key"),
                  secret_key=cfg.get("minio", "secret_key"), secure=secure)
    if secure and insecure:
        import urllib3
        urllib3.disable_warnings()
        kwargs["http_client"] = urllib3.PoolManager(cert_reqs="CERT_NONE")
    return Minio(cfg.get("minio", "endpoint"), **kwargs), cfg.get("minio", "raw_bucket")


def ensure_tables(catalog, namespace: str = NAMESPACE_DEFAULT) -> dict:
    """Create the namespace and both tables (day-partitioned) if missing.
    Returns {table_name: Table}. Idempotent."""
    from pyiceberg.exceptions import NamespaceAlreadyExistsError
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass
    return {name: ensure_table(catalog, namespace, name, fields)
            for name, fields in TABLE_FIELDS.items()}


def table_arrow_batch(table, df: pd.DataFrame):
    """Cast a raw parquet DataFrame to the table's Iceberg arrow schema.
    Drops hive partition helper columns and normalises ts to microseconds."""
    import pyarrow as pa
    df = df.drop(columns=[c for c in ("dt", "hour") if c in df.columns], errors="ignore")
    if "ts" in df.columns:
        df = df.assign(ts=df["ts"].astype("datetime64[us]"))
    return pa.Table.from_pandas(df, schema=table.schema().as_arrow(), preserve_index=False)


def table_for_key(key: str) -> str | None:
    """Route a raw object key to its target table by prefix."""
    base = key.replace("\\", "/")
    if "app_events/" in base or base.startswith("app_events"):
        return "app_events"
    if "vm_metrics/" in base or base.startswith("vm_metrics"):
        return "vm_metrics"
    return None


# --------------------------------------------------------------------------- #
# Processed manifest (exactly-once). Storage-agnostic for testability.
# --------------------------------------------------------------------------- #
class MemoryManifestStore:
    """In-memory manifest store (tests / dry runs)."""
    def __init__(self, blob: bytes | None = None):
        self._blob = blob

    def get(self) -> bytes | None:
        return self._blob

    def put(self, blob: bytes) -> None:
        self._blob = blob


class LocalManifestStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def get(self) -> bytes | None:
        return self.path.read_bytes() if self.path.exists() else None

    def put(self, blob: bytes) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(blob)


class S3ManifestStore:
    """Stores the manifest as a single object in the raw bucket."""
    def __init__(self, client, bucket: str, key: str = "_manifest/ingest_processed.json"):
        self.client = client
        self.bucket = bucket
        self.key = key

    def get(self) -> bytes | None:
        from minio.error import S3Error
        try:
            resp = self.client.get_object(self.bucket, self.key)
            try:
                return resp.read()
            finally:
                resp.close()
                resp.release_conn()
        except S3Error:
            return None

    def put(self, blob: bytes) -> None:
        self.client.put_object(self.bucket, self.key, io.BytesIO(blob), length=len(blob),
                               content_type="application/json")


class ProcessedManifest:
    """Tracks which raw object keys have already been appended."""
    def __init__(self, store):
        self.store = store
        raw = store.get()
        doc = json.loads(raw) if raw else {"processed": []}
        self._keys: set[str] = set(doc.get("processed", []))

    def __contains__(self, key: str) -> bool:
        return key in self._keys

    def add(self, key: str) -> None:
        self._keys.add(key)

    def save(self) -> None:
        blob = json.dumps({"processed": sorted(self._keys)}, indent=0).encode()
        self.store.put(blob)

    @property
    def count(self) -> int:
        return len(self._keys)


# --------------------------------------------------------------------------- #
# Raw sources
# --------------------------------------------------------------------------- #
@dataclass
class RawObject:
    key: str
    table: str

    def read(self) -> pd.DataFrame:  # pragma: no cover - overridden per source
        raise NotImplementedError


class LocalRawSource:
    """Reads chunk parquet files under a local data/ root."""
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def list_objects(self) -> list[RawObject]:
        objs = []
        for tbl in TABLE_FIELDS:
            for p in sorted((self.root / tbl).rglob("*.parquet")):
                key = str(p.relative_to(self.root))
                objs.append(_LocalObject(key=key, table=tbl, path=p))
        return objs


@dataclass
class _LocalObject(RawObject):
    path: Path = None

    def read(self) -> pd.DataFrame:
        return pd.read_parquet(self.path)


class S3RawSource:
    """Lists and reads chunk parquet objects from the raw bucket."""
    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    def list_objects(self) -> list[RawObject]:
        objs = []
        for tbl in TABLE_FIELDS:
            for obj in self.client.list_objects(self.bucket, prefix=f"{tbl}/", recursive=True):
                if obj.object_name.endswith(".parquet"):
                    objs.append(_S3Object(key=obj.object_name, table=tbl,
                                          client=self.client, bucket=self.bucket))
        return objs


@dataclass
class _S3Object(RawObject):
    client: object = None
    bucket: str = None

    def read(self) -> pd.DataFrame:
        resp = self.client.get_object(self.bucket, self.key)
        try:
            return pd.read_parquet(io.BytesIO(resp.read()))
        finally:
            resp.close()
            resp.release_conn()


# --------------------------------------------------------------------------- #
# Ingest loop
# --------------------------------------------------------------------------- #
@dataclass
class AppendResult:
    key: str
    table: str
    rows: int
    snapshot_id: int


def ingest_once(tables: dict, manifest: ProcessedManifest, source) -> list[AppendResult]:
    """Append every not-yet-processed raw object exactly once.
    One append == one snapshot. Persists the manifest after each append so a
    crash never re-appends a committed chunk."""
    results: list[AppendResult] = []
    for obj in source.list_objects():
        if obj.key in manifest:
            continue
        table = tables.get(obj.table)
        if table is None:
            continue
        df = obj.read()
        if df.empty:
            manifest.add(obj.key)
            manifest.save()
            continue
        batch = table_arrow_batch(table, df)
        table.append(batch)
        table.refresh()
        snap = table.current_snapshot()
        manifest.add(obj.key)
        manifest.save()
        results.append(AppendResult(key=obj.key, table=obj.table, rows=len(df),
                                    snapshot_id=snap.snapshot_id if snap else -1))
    return results
