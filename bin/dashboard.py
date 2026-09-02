#!/usr/bin/env python3
"""Read-only AIOps dashboard over the telemetry lake (PLAN M6).

Reads live from AIStor Iceberg tables (via the REST catalog) by default, or
local parquet. Shows a fleet-health grid, a per-VM heap chart with the OOM
ceiling, and the live alerts table. The "as of" slider replays time: drag it
back and watch the heap-leak P1 appear ~45 min before the crash.

Run:
  streamlit run bin/dashboard.py
  streamlit run bin/dashboard.py -- --source local     # args after `--`
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from aiops import detect, ingest
from aiops.config import REPO_ROOT, load_config

STATUS_COLORS = {"healthy": "#2ecc71", "P2": "#f39c12", "P1": "#e74c3c"}
SEV_RANK = {"P1": 2, "P2": 1}

st.set_page_config(page_title="AIOps Lakehouse", page_icon="🚨", layout="wide")


@st.cache_data(show_spinner="Reading the lake…")
def load_lake(source: str, insecure: bool):
    """Pull vm_metrics + app_events into DataFrames (cached until reload)."""
    cfg = load_config()
    if source == "iceberg":
        con = detect.duckdb_from_iceberg(cfg, ingest.NAMESPACE_DEFAULT, insecure=insecure)
    else:
        con = detect.duckdb_from_local(str(REPO_ROOT / "data"))
    vm = con.execute("SELECT * FROM vm_metrics").fetch_df()
    ev = con.execute("SELECT * FROM app_events").fetch_df()
    return vm, ev


def compute(vm_df, ev_df, asof_min):
    """Register the cached frames and run the detector at the chosen asof."""
    cfg = load_config()
    dc = detect.DetectConfig.from_ini(cfg)
    con = duckdb.connect()
    con.register("vm_metrics", vm_df)
    con.register("app_events", ev_df)
    t_end = detect.evaluation_end(con, asof_min)
    alerts = detect.detect(con, dc, asof_min=asof_min)
    return alerts, t_end, dc


# ---- sidebar ---------------------------------------------------------------
st.sidebar.title("🚨 AIOps Lakehouse")
default_source = "local" if "--source" in sys.argv and "local" in sys.argv else "iceberg"
source = st.sidebar.radio("Data source", ["iceberg", "local"],
                          index=0 if default_source == "iceberg" else 1,
                          help="iceberg = live AIStor tables via the REST catalog")
insecure = st.sidebar.checkbox("TLS --insecure (lab routes)", value=False)
asof = st.sidebar.slider("As of — minutes before data end", 0, 90, 15, step=5,
                         help="Replay: how far back to pretend 'now' is")
if st.sidebar.button("↻ Reload from lake"):
    st.cache_data.clear()
    st.rerun()

# ---- load + detect ---------------------------------------------------------
try:
    vm_df, ev_df = load_lake(source, insecure)
except Exception as e:
    st.error(f"Could not read the lake ({source}): {e}")
    st.stop()

if vm_df.empty:
    st.warning("vm_metrics is empty — generate + load data first.")
    st.stop()

alerts, t_end, dc = compute(vm_df, ev_df, asof)

# ---- header metrics --------------------------------------------------------
st.title("Fleet health")
st.caption(f"Source: **{source}**  ·  evaluating as of **{t_end:%Y-%m-%d %H:%M:%S}** "
           f"(T-{asof}m)  ·  {vm_df['vm_id'].nunique()} VMs  ·  {len(vm_df):,} samples")

p1 = int((alerts["severity"] == "P1").sum()) if not alerts.empty else 0
p2 = int((alerts["severity"] == "P2").sum()) if not alerts.empty else 0
m1, m2, m3, m4 = st.columns(4)
m1.metric("VMs", vm_df["vm_id"].nunique())
m2.metric("🔴 P1 alerts", p1)
m3.metric("🟠 P2 alerts", p2)
m4.metric("Apps", vm_df["app"].nunique())

# ---- fleet health grid -----------------------------------------------------
vm_rank: dict[str, int] = {}
if not alerts.empty:
    for r in alerts.itertuples(index=False):
        if r.vm_id:
            vm_rank[r.vm_id] = max(vm_rank.get(r.vm_id, 0), SEV_RANK.get(r.severity, 0))

grid = vm_df[["vm_id", "app"]].drop_duplicates().copy()
grid["rank"] = grid["vm_id"].map(lambda v: vm_rank.get(v, 0))
grid["status"] = grid["rank"].map({0: "healthy", 1: "P2", 2: "P1"})
grid["i"] = grid.groupby("app").cumcount()

st.subheader("Fleet grid — one square per VM")
fleet = (alt.Chart(grid).mark_rect(stroke="white", strokeWidth=1).encode(
    x=alt.X("i:O", axis=None),
    y=alt.Y("app:N", title=None, sort="-x"),
    color=alt.Color("status:N",
                    scale=alt.Scale(domain=list(STATUS_COLORS),
                                    range=list(STATUS_COLORS.values())),
                    legend=alt.Legend(title="status")),
    tooltip=["vm_id", "app", "status"],
).properties(height=28 * grid["app"].nunique() + 40))
st.altair_chart(fleet, width='stretch')

# ---- heap chart + alerts ---------------------------------------------------
left, right = st.columns([3, 4])

with left:
    st.subheader("Heap vs OOM ceiling")
    leak_vms = (alerts[alerts["rule"] == "heap_leak"]["vm_id"].tolist()
                if not alerts.empty else [])
    vm_ids = sorted(grid["vm_id"])
    default_idx = vm_ids.index(leak_vms[0]) if leak_vms else 0
    vm_sel = st.selectbox("VM", vm_ids, index=default_idx)

    s = vm_df[vm_df["vm_id"] == vm_sel][["ts", "heap_mb"]].sort_values("ts")
    s = s.assign(phase=["known" if t <= t_end else "after-now" for t in s["ts"]])
    base = alt.Chart(s).encode(x=alt.X("ts:T", title=None))
    line = base.mark_line().encode(
        y=alt.Y("heap_mb:Q", title="heap MB"),
        color=alt.Color("phase:N",
                        scale=alt.Scale(domain=["known", "after-now"],
                                        range=["#3498db", "#b0bec5"]),
                        legend=None))
    cap = alt.Chart(pd.DataFrame({"y": [dc.heap_cap_mb]})).mark_rule(
        color="#e74c3c", strokeDash=[6, 4]).encode(y="y:Q")
    now = alt.Chart(pd.DataFrame({"x": [pd.Timestamp(t_end)]})).mark_rule(
        color="#555", strokeDash=[2, 2]).encode(x="x:T")
    st.altair_chart(line + cap + now, width='stretch')
    st.caption("Red dashed = OOM ceiling · grey line = 'now' (asof) · "
               "blue = known, grey = after-now")

with right:
    st.subheader(f"Live alerts ({len(alerts)})")
    if alerts.empty:
        st.success("No active alerts — fleet healthy at this time.")
    else:
        show = alerts[["severity", "rule", "vm_id", "app", "headline"]].copy()
        st.dataframe(
            show.style.apply(
                lambda row: [f"background-color: {STATUS_COLORS.get(row.severity, '')}22"] * len(row),
                axis=1),
            width='stretch', hide_index=True, height=460)
