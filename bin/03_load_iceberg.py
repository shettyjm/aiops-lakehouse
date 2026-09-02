#!/usr/bin/env python3
"""Load raw telemetry chunks into AIStor Iceberg Tables (milestone M2).

Appends each new chunk exactly once (tracked in a processed-manifest), one
snapshot per chunk. Raw chunks come from the AIStor raw bucket (--source s3,
default) or local parquet under ./data (--source local). Target is always the
Iceberg tables.

Examples:
  bin/03_load_iceberg.py --source s3 --once
  bin/03_load_iceberg.py --source s3 --watch --interval 10 --insecure
  bin/03_load_iceberg.py --source local --once
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiops import ingest  # noqa: E402
from aiops.config import REPO_ROOT, load_config  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["s3", "local"], default="s3",
                   help="where raw chunks come from (default s3)")
    p.add_argument("--once", action="store_true", help="single pass (default)")
    p.add_argument("--watch", action="store_true", help="poll continuously")
    p.add_argument("--interval", type=float, default=10.0, help="watch poll seconds")
    p.add_argument("--namespace", default=ingest.NAMESPACE_DEFAULT)
    p.add_argument("--data", default=str(REPO_ROOT / "data"), help="local data root")
    p.add_argument("--config", default=str(REPO_ROOT / "config.ini"))
    p.add_argument("--insecure", action="store_true", help="disable TLS verification")
    return p.parse_args(argv)


def _build_source_and_manifest(args, cfg):
    if args.source == "s3":
        client, bucket = ingest.minio_client(cfg, args.insecure)
        source = ingest.S3RawSource(client, bucket)
        store = ingest.S3ManifestStore(client, bucket)
        where = f"s3://{bucket}/"
    else:
        source = ingest.LocalRawSource(args.data)
        store = ingest.LocalManifestStore(Path(args.data) / "_manifest" / "ingest_processed.json")
        where = str(Path(args.data))
    return source, ingest.ProcessedManifest(store), where


def _read_dimension(args, cfg, name):
    """Read a static dimension parquet (topology/sites) from s3 or local."""
    import io
    import pandas as pd
    if args.source == "s3":
        client, bucket = ingest.minio_client(cfg, args.insecure)
        try:
            resp = client.get_object(bucket, f"{name}/{name}.parquet")
            try:
                return pd.read_parquet(io.BytesIO(resp.read()))
            finally:
                resp.close(); resp.release_conn()
        except Exception:
            return None
    import glob
    files = glob.glob(f"{args.data}/{name}/*.parquet")
    return pd.read_parquet(files[0]) if files else None


def _load_dimensions(args, cfg, catalog) -> None:
    """Load/overwrite the static topology + sites dimensions if present."""
    for name, loader in (("topology", ingest.load_topology),
                         ("sites", ingest.load_sites)):
        df = _read_dimension(args, cfg, name)
        if df is not None and not df.empty:
            n = loader(catalog, df, args.namespace)
            print(f"    {name}: overwrote {n} rows")


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)

    print(f"==> connecting to Iceberg catalog {cfg.get('iceberg','uri')}")
    catalog = ingest.iceberg_catalog(cfg, insecure=args.insecure)
    tables = ingest.ensure_tables(catalog, args.namespace)
    print(f"    namespace {args.namespace}: tables {', '.join(tables)}")

    # Static dimensions (topology, sites): load/overwrite once if present.
    _load_dimensions(args, cfg, catalog)

    source, manifest, where = _build_source_and_manifest(args, cfg)
    print(f"==> ingesting from {where} (manifest has {manifest.count} processed)")

    def one_pass() -> int:
        results = ingest.ingest_once(tables, manifest, source)
        for r in results:
            print(f"    appended {r.rows:>6} rows -> {r.table:<10} snapshot={r.snapshot_id}  [{r.key}]")
        if not results:
            print("    (no new chunks)")
        return len(results)

    if args.watch:
        print(f"==> watch mode, polling every {args.interval}s (Ctrl-C to stop)")
        try:
            while True:
                one_pass()
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n==> stopped")
    else:
        one_pass()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
