"""Render benchmark results (results.json payload) into a Markdown report and run
the cross-engine correctness check."""

from __future__ import annotations

from pathlib import Path


def _engines(payload: dict) -> list[dict]:
    return [e for e in payload["engines"] if "error" not in e]


def write_report(payload: dict, path: Path) -> None:
    lines: list[str] = []
    lines.append("# Benchmark results")
    lines.append("")
    lines.append(f"- Scale: **{payload['scale']}**  |  warm repetitions: {payload['warm']}")
    versions = ", ".join(
        f"{e['engine']} {e['engine_version']}" for e in payload["engines"] if e.get("engine_version")
    )
    if versions:
        lines.append(f"- Engine versions: {versions}")
    lines.append("")

    engines = payload["engines"]
    ok = _engines(payload)

    # ---- Ingestion + resources -------------------------------------------------
    lines.append("## Ingestion & resources")
    lines.append("")
    lines.append("| Engine | Ingestion (s) | Peak mem (MB) | Mean CPU (%) | Note |")
    lines.append("|---|--:|--:|--:|---|")
    for e in engines:
        if "error" in e:
            lines.append(f"| {e['engine']} | — | — | — | ERROR: {e['error']} |")
            continue
        r = e.get("resources", {})
        lines.append(
            f"| {e['engine']} | {e.get('ingest_seconds', '—')} | "
            f"{r.get('peak_mem_mb', '—')} | {r.get('mean_cpu_pct', '—')} | {e.get('ingest_note', '')} |"
        )
    lines.append("")

    etl = payload.get("etl_conversion") or {}
    if etl.get("seconds") is not None:
        lines.append(
            f"> **ETL Beacon avoids:** converting the source NetCDF to Parquet (which the "
            f"SQL engines require) took **{etl['seconds']}s** for {etl.get('rows', '?')} rows. "
            f"Beacon queries the NetCDF directly (`beacon-netcdf`), paying none of this."
        )
        lines.append("")
    elif etl.get("skipped"):
        lines.append(f"> ETL conversion measurement skipped: {etl['skipped']}")
        lines.append("")

    # ---- Latency per query -----------------------------------------------------
    qkeys = list(payload["queries"].keys())
    lines.append("## Query latency — warm p50 (ms)")
    lines.append("")
    header = "| Query | " + " | ".join(e["engine"] for e in ok) + " |"
    lines.append(header)
    lines.append("|---" * (len(ok) + 1) + "|")
    for qk in qkeys:
        row = [payload["queries"][qk]]
        for e in ok:
            q = e["queries"].get(qk, {})
            if "error" in q:
                row.append("ERR")
            else:
                row.append(str(q.get("warm", {}).get("p50_ms", "—")))
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    lines.append("## Query latency — cold (ms)")
    lines.append("")
    lines.append(header)
    lines.append("|---" * (len(ok) + 1) + "|")
    for qk in qkeys:
        row = [payload["queries"][qk]]
        for e in ok:
            q = e["queries"].get(qk, {})
            row.append("ERR" if "error" in q else str(q.get("cold_ms", "—")))
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    # ---- Correctness -----------------------------------------------------------
    lines.append("## Correctness check (result row counts must match across engines)")
    lines.append("")
    lines.append("| Query | " + " | ".join(e["engine"] for e in ok) + " | consistent? |")
    lines.append("|---" * (len(ok) + 2) + "|")
    for qk in qkeys:
        counts = []
        row = [payload["queries"][qk]]
        for e in ok:
            q = e["queries"].get(qk, {})
            c = q.get("result_rows")
            row.append(str(c) if c is not None else "—")
            if c is not None:
                counts.append(c)
        consistent = "✅" if len(set(counts)) <= 1 and counts else "❌"
        row.append(consistent)
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def print_correctness(payload: dict) -> None:
    ok = _engines(payload)
    print("\nCorrectness (result row counts):")
    all_good = True
    for qk, title in payload["queries"].items():
        counts = {e["engine"]: e["queries"].get(qk, {}).get("result_rows") for e in ok}
        vals = [c for c in counts.values() if c is not None]
        consistent = len(set(vals)) <= 1 and bool(vals)
        all_good &= consistent
        flag = "OK " if consistent else "MISMATCH"
        print(f"  [{flag}] {qk:18s} {counts}")
    print("  => " + ("all engines agree" if all_good else "MISMATCHES FOUND"))
