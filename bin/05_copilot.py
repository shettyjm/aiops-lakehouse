#!/usr/bin/env python3
"""Sovereign SRE copilot — ask the lake in plain English (milestone M4).

Examples:
  bin/05_copilot.py "why is patient onboarding slow?"                 # config backend
  bin/05_copilot.py "why is patient onboarding slow?" --backend ollama
  bin/05_copilot.py "what's wrong with postgres?" --backend none      # no model, SQL only
  bin/05_copilot.py "why is onboarding slow?" --backend both          # local vs Claude

With no model reachable, the copilot still answers from a deterministic SQL
root-cause plan, so the demo never dies.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiops import copilot  # noqa: E402
from aiops.config import REPO_ROOT, load_config  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("question", help="natural-language question about the fleet")
    p.add_argument("--backend", default=None,
                   help="ollama|openshift_ai|claude|none|both (default: config [model])")
    p.add_argument("--source", choices=["local", "iceberg"], default="iceberg")
    p.add_argument("--asof", type=float, default=0.0)
    p.add_argument("--namespace", default=copilot.ingest.NAMESPACE_DEFAULT)
    p.add_argument("--data", default=str(REPO_ROOT / "data"))
    p.add_argument("--config", default=str(REPO_ROOT / "config.ini"))
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--show-tools", action="store_true", help="print tool calls made")
    return p.parse_args(argv)


def _emit(res, show_tools: bool):
    tag = f"[{res.backend}{' · fallback' if res.used_fallback else ''}]"
    print(tag)
    if show_tools and res.tool_calls:
        for name, args in res.tool_calls:
            print(f"  · tool {name}({args})")
    print(res.text)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)
    backend = args.backend or cfg.get("model", "backend", fallback="none")

    tools = copilot.open_lake(cfg, args.source, args.data, args.namespace, args.insecure)

    print(f"Q: {args.question}\n{'='*70}")
    if backend == "both":
        local = cfg.get("model", "backend", fallback="ollama")
        local = local if local in ("ollama", "openshift_ai") else "ollama"
        for b in (local, "claude"):
            print(f"\n----- backend: {b} -----")
            _emit(copilot.answer(args.question, tools, cfg, b, args.asof), args.show_tools)
    else:
        _emit(copilot.answer(args.question, tools, cfg, backend, args.asof), args.show_tools)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
