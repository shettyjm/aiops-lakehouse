#!/usr/bin/env python3
"""Run the detection engine and deliver alerts (milestone M3).

Reads the lake (local parquet or Iceberg), evaluates all rules as-of
(data end - --asof minutes), writes alerts to data/alerts.json and/or the
Iceberg alerts table, and optionally posts a Slack-format webhook.

Examples:
  bin/04_detect.py --source local
  bin/04_detect.py --source iceberg --asof 45 --alerts-to json
  bin/04_detect.py --source iceberg --webhook https://hooks.slack.com/... --insecure
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiops import detect  # noqa: E402
from aiops.config import REPO_ROOT, load_config  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["local", "iceberg"], default="local")
    p.add_argument("--asof", type=float, default=0.0,
                   help="evaluate as of N minutes before data end (replay)")
    p.add_argument("--alerts-to", choices=["both", "json", "iceberg"], default="both")
    p.add_argument("--webhook", default=None, help="Slack-format webhook URL")
    p.add_argument("--namespace", default=detect.ingest.NAMESPACE_DEFAULT)
    p.add_argument("--data", default=str(REPO_ROOT / "data"))
    p.add_argument("--out", default=str(REPO_ROOT / "data" / "alerts.json"))
    p.add_argument("--config", default=str(REPO_ROOT / "config.ini"))
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--quiet", action="store_true", help="only print the summary line")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    dc = detect.DetectConfig.from_ini(cfg)

    if args.source == "local":
        con = detect.duckdb_from_local(args.data)
    else:
        con = detect.duckdb_from_iceberg(cfg, args.namespace, insecure=args.insecure)

    t_end = detect.evaluation_end(con, args.asof)
    alerts = detect.detect(con, dc, asof_min=args.asof)

    print(f"==> detect asof -{args.asof:.0f}m (eval end {t_end:%Y-%m-%d %H:%M:%S}) "
          f"-> {len(alerts)} alert(s)")
    if not args.quiet:
        for r in alerts.itertuples(index=False):
            who = r.vm_id or r.app
            print(f"    [{r.severity}] {r.rule:<11} {who:<24} {r.headline}")

    # Sinks
    if args.alerts_to in ("both", "json"):
        detect.write_alerts_json(alerts, args.out)
        print(f"    wrote {args.out}")
    if args.alerts_to in ("both", "iceberg"):
        try:
            n = detect.write_alerts_iceberg(cfg, alerts, args.namespace,
                                            insecure=args.insecure)
            print(f"    appended {n} alert(s) to Iceberg {args.namespace}.alerts")
        except Exception as e:  # keep the demo alive if the catalog hiccups
            print(f"    WARN: could not write Iceberg alerts: {str(e)[:80]}")
    if args.webhook:
        try:
            detect.post_webhook(args.webhook, alerts, args.asof)
            print("    posted webhook")
        except Exception as e:
            print(f"    WARN: webhook failed: {str(e)[:80]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
