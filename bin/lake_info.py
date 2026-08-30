#!/usr/bin/env python3
"""Inspect the AIStor Iceberg lake (milestone M2).

Lists tables, row counts, the last 10 snapshots with timestamps, and a
time-travel example (row count as-of the previous snapshot vs current).

Usage:
  bin/lake_info.py [--namespace observability] [--insecure]
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiops import ingest  # noqa: E402
from aiops.config import REPO_ROOT, load_config  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--namespace", default=ingest.NAMESPACE_DEFAULT)
    p.add_argument("--config", default=str(REPO_ROOT / "config.ini"))
    p.add_argument("--insecure", action="store_true")
    return p.parse_args(argv)


def _fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _snapshot_rows(table, snapshot_id) -> int:
    """total-records from the snapshot summary if present, else a full scan."""
    for snap in table.metadata.snapshots:
        if snap.snapshot_id == snapshot_id:
            total = (snap.summary.additional_properties or {}).get("total-records") \
                if snap.summary else None
            if total is not None:
                return int(total)
    return table.scan(snapshot_id=snapshot_id).to_arrow().num_rows


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    catalog = ingest.iceberg_catalog(cfg, insecure=args.insecure)

    print(f"warehouse: {cfg.get('iceberg','warehouse')}   namespace: {args.namespace}\n")
    idents = catalog.list_tables(args.namespace)
    if not idents:
        print("(no tables yet — run bin/03_load_iceberg.py)")
        return 0

    for ident in idents:
        table = catalog.load_table(ident)
        name = ident[-1]
        snaps = list(table.metadata.snapshots)
        cur = table.current_snapshot()
        cur_rows = _snapshot_rows(table, cur.snapshot_id) if cur else 0

        print(f"=== {name} ===")
        print(f"    rows (current): {cur_rows:,}   snapshots: {len(snaps)}")

        print("    last 10 snapshots (newest last):")
        for snap in snaps[-10:]:
            summ = (snap.summary.additional_properties or {}) if snap.summary else {}
            op = snap.summary.operation if snap.summary else "?"
            added = summ.get("added-records", "?")
            print(f"      {snap.snapshot_id}  {_fmt_ts(snap.timestamp_ms)}  "
                  f"{op:<8} +{added} rows")

        # Time-travel example: rows as-of the previous snapshot.
        if len(snaps) >= 2:
            prev = snaps[-2]
            prev_rows = _snapshot_rows(table, prev.snapshot_id)
            print(f"    time-travel: as-of snapshot {prev.snapshot_id} "
                  f"({_fmt_ts(prev.timestamp_ms)}) -> {prev_rows:,} rows "
                  f"(current {cur_rows:,}, delta +{cur_rows - prev_rows:,})")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
