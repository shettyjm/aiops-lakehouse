#!/usr/bin/env python3
"""Generate synthetic 2,000-VM telemetry (milestone M1).

Two output modes:
  * batch (default): write the whole window as hive-partitioned parquet under
    ./data/vm_metrics and ./data/app_events.
  * --stream: slice the window into --chunk-min chunks and emit one every
    --stream-interval-s seconds. With --upload each chunk is PUT to
    s3://<raw_bucket>/ on the AIStor endpoint from config.ini, simulating 2,000
    agents shipping without running 2,000 agents.

Examples:
  bin/02_generate.py --hours 2 --seed 42
  bin/02_generate.py --stream --chunk-min 10 --stream-interval-s 5 --upload --insecure
"""

from __future__ import annotations

import argparse
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Allow running as a script without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiops import generator as gen  # noqa: E402
from aiops.config import REPO_ROOT, get_bool, load_config  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hours", type=float, default=2.0, help="window length (default 2)")
    p.add_argument("--interval", type=int, default=None,
                   help="sample interval seconds (default from fleet.yaml)")
    p.add_argument("--start", default=None,
                   help="ISO8601 window start (UTC); default = now - hours, "
                        "aligned to the interval")
    p.add_argument("--seed", type=int, default=42, help="RNG seed (deterministic)")
    p.add_argument("--max-vms", type=int, default=None,
                   help="scale the fleet down to ~N VMs total, preserving app "
                        "proportions (min 1 per app). Default: full fleet.yaml counts.")
    p.add_argument("--fleet", default=str(REPO_ROOT / "fleet.yaml"))
    p.add_argument("--scenarios", default=str(REPO_ROOT / "scenarios.yaml"))
    p.add_argument("--out", default=str(REPO_ROOT / "data"), help="output root dir")
    p.add_argument("--config", default=str(REPO_ROOT / "config.ini"))
    p.add_argument("--stream", action="store_true", help="emit chunk-by-chunk")
    p.add_argument("--chunk-min", type=float, default=10.0,
                   help="stream chunk length in minutes (default 10)")
    p.add_argument("--stream-interval-s", type=float, default=5.0,
                   help="seconds to sleep between streamed chunks")
    p.add_argument("--upload", action="store_true",
                   help="in --stream mode, PUT each chunk to the raw bucket")
    p.add_argument("--insecure", action="store_true", help="disable TLS verification")
    return p.parse_args(argv)


def _resolve_start(arg_start: str | None, interval_s: int, hours: float) -> datetime:
    if arg_start:
        dt = datetime.fromisoformat(arg_start)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    # Default: now - hours, floored to the interval for tidy partitions.
    now = datetime.now(timezone.utc).replace(microsecond=0)
    epoch = int(now.timestamp())
    floored = epoch - (epoch % interval_s)
    return datetime.fromtimestamp(floored - int(hours * 3600), tz=timezone.utc)


def _minio_client(cfg, insecure: bool):
    from minio import Minio
    endpoint = cfg.get("minio", "endpoint")
    secure = get_bool(cfg, "minio", "secure", fallback=True)
    kwargs = dict(
        access_key=cfg.get("minio", "access_key"),
        secret_key=cfg.get("minio", "secret_key"),
        secure=secure,
    )
    if secure and insecure:
        import urllib3
        urllib3.disable_warnings()
        kwargs["http_client"] = urllib3.PoolManager(cert_reqs="CERT_NONE")
    return Minio(endpoint, **kwargs), cfg.get("minio", "raw_bucket")


def _parquet_bytes(df) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf)
    return buf.getvalue()


def _upload_chunk(client, bucket, table, df, chunk_idx) -> str:
    """PUT one chunk as a single parquet object under a hive-style key."""
    ts0 = df["ts"].min()
    d = ts0.strftime("%Y-%m-%d")
    h = ts0.strftime("%H")
    key = f"{table}/dt={d}/hour={h}/part-{chunk_idx:04d}.parquet"
    body = _parquet_bytes(df)
    client.put_object(bucket, key, io.BytesIO(body), length=len(body),
                      content_type="application/vnd.apache.parquet")
    return key


def main(argv=None) -> int:
    args = parse_args(argv)

    fleet_spec = gen.load_fleet(args.fleet)
    scenarios = gen.load_scenarios(args.scenarios)
    interval_s = args.interval or int(fleet_spec["sample_interval_s"])
    fleet_spec["sample_interval_s"] = interval_s

    if args.max_vms is not None:
        actual = gen.scale_fleet(fleet_spec, args.max_vms)
        print(f"==> scaled fleet to {actual} VMs (requested ~{args.max_vms})")

    n_samples = int(round(args.hours * 3600 / interval_s))
    if n_samples <= 0:
        print("nothing to generate (hours too small)", file=sys.stderr)
        return 2
    start = _resolve_start(args.start, interval_s, args.hours)

    print(f"==> generating {args.hours}h @ {interval_s}s "
          f"({n_samples} samples) for {sum(a['count'] for a in fleet_spec['apps'].values())} VMs")
    print(f"    window: {start.isoformat()} .. seed={args.seed}")

    result = gen.generate(fleet_spec, scenarios, start=start,
                          n_samples=n_samples, seed=args.seed)

    n_metric_rows = len(result.metrics)
    n_event_rows = len(result.events)
    print(f"    metrics rows: {n_metric_rows:,}   event rows: {n_event_rows:,}")
    for name, vms in result.incident_vms.items():
        print(f"    incident {name}: {', '.join(vms) if vms else '(no targets)'}")

    if not args.stream:
        gen.write_dataset(result.metrics, args.out, "vm_metrics")
        gen.write_dataset(result.events, args.out, "app_events")
        gen.write_flat(result.topology, args.out, "topology")
        gen.write_flat(result.sites, args.out, "sites")
        print(f"==> wrote parquet under {Path(args.out)}/vm_metrics, /app_events, "
              f"/topology ({len(result.topology)} edges), /sites ({len(result.sites)})")
        return 0

    # --- stream mode ---------------------------------------------------------
    client = bucket = None
    if args.upload:
        cfg = load_config(args.config)
        client, bucket = _minio_client(cfg, args.insecure)
        print(f"==> streaming with upload -> s3://{bucket}/")
    else:
        print("==> streaming (local write only; pass --upload to ship to AIStor)")

    # Static dimensions — write/upload once, not per chunk.
    for name, dim in (("topology", result.topology), ("sites", result.sites)):
        if dim.empty:
            continue
        if args.upload:
            body = _parquet_bytes(dim)
            client.put_object(bucket, f"{name}/{name}.parquet", io.BytesIO(body),
                              length=len(body), content_type="application/vnd.apache.parquet")
            print(f"    {name}: {len(dim)} rows -> s3://{bucket}/{name}/{name}.parquet")
        else:
            gen.write_flat(dim, args.out, name)

    samples_per_chunk = max(int(round(args.chunk_min * 60 / interval_s)), 1)
    chunk_idx = 0
    for c0 in range(0, n_samples, samples_per_chunk):
        c1 = min(c0 + samples_per_chunk, n_samples)
        t_lo, t_hi = result.ts[c0], result.ts[c1 - 1]
        m_chunk = result.metrics[(result.metrics["ts"] >= t_lo) & (result.metrics["ts"] <= t_hi)]
        e_chunk = result.events[(result.events["ts"] >= t_lo) & (result.events["ts"] <= t_hi)] \
            if not result.events.empty else result.events

        if args.upload:
            mk = _upload_chunk(client, bucket, "vm_metrics", m_chunk, chunk_idx)
            msg = f"    chunk {chunk_idx:04d}: {len(m_chunk):,} rows -> s3://{bucket}/{mk}"
            if not e_chunk.empty:
                ek = _upload_chunk(client, bucket, "app_events", e_chunk, chunk_idx)
                msg += f"  (+{len(e_chunk)} events)"
            print(msg)
        else:
            gen.write_dataset(m_chunk, args.out, "vm_metrics", basename=f"chunk{chunk_idx:04d}")
            if not e_chunk.empty:
                gen.write_dataset(e_chunk, args.out, "app_events", basename=f"chunk{chunk_idx:04d}")
            print(f"    chunk {chunk_idx:04d}: {len(m_chunk):,} rows written")

        chunk_idx += 1
        if c1 < n_samples and args.stream_interval_s > 0:
            time.sleep(args.stream_interval_s)

    print(f"==> streamed {chunk_idx} chunks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
