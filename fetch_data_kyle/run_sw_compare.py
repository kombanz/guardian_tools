#!/usr/bin/env python3
"""run_sw_compare.py

Repeatable 11.4 (3.15.46) vs 12.2 (12.2.20) performance comparison.

This script is designed for regular re-runs on Guardian Live CSV exports (events + trips).
It:
  - Loads & normalizes Events and Trips CSVs
  - Applies trip-level exposure filtering rules (for denominators only)
  - Excludes low-exposure units (<min_hours mobile hours in either version)
  - Computes pooled metrics by software_version for Microsleep (2 TP variants) and LGA
  - Produces per-version Top-N "worst FP/hr" unit lists
  - Exports all outputs to a single Excel workbook with consistent sheet names
  - Optionally produces daily (date columns) tables

Key notes:
  - Rates are per MOBILE hour (not operating hour)
  - Trip filtering applies ONLY to exposure denominators (not to event numerators)
  - Pooled metrics & unit rankings use ONLY units meeting the min-hours rule in BOTH versions

Example:
  python run_sw_compare.py --events 14973_events_report_20260317.csv --trips 14973_trips_report_20260317.csv --out results_20260317.xlsx --daily pooled

Dependencies:
  pip install pandas openpyxl numpy
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Configuration / constants
# -----------------------------

DEFAULT_VERSIONS = ["3.15.46", "12.2.20"]
DEFAULT_MIN_HOURS = 30.0
DEFAULT_TOPN = 10

# Trip filtering rules (for exposure only)
TRIP_RULE_MIN_MOBILE_MINS = 4
TRIP_RULE_EXTREME_MOBILE_MINS = 2000
TRIP_RULE_EXTREME_MAX_DISTANCE_KMS = 200

# Event types
EV_MS = "microsleep"
EV_LGA = "lga"
EV_VATS = "vats"

# Classification sets (lowercased)
MS_TP_BASE = {"microsleep", "drowsiness"}
MS_TP_YAWN = {"microsleep", "drowsiness", "yawning"}
LGA_TP = {"long glance away", "mobile device", "other distraction"}


# -----------------------------
# Utility helpers
# -----------------------------

def _as_str_series(s: pd.Series) -> pd.Series:
    """Convert to string series while preserving NaNs."""
    # Using pandas string dtype can be finicky across versions; keep as object strings.
    return s.astype("string")


def norm_token(s: pd.Series) -> pd.Series:
    """Lowercase/strip tokens used for keys (software_version, guardian_unit, event_type, classification)."""
    s = _as_str_series(s)
    s = s.str.strip().str.lower()
    # Normalize common null strings
    s = s.replace({"nan": pd.NA, "none": pd.NA, "": pd.NA})
    return s


def clean_display_text(s: pd.Series) -> pd.Series:
    """Canonical cleaner for display names (account/fleet).

    - removes CR/LF/Tabs
    - collapses internal whitespace
    - normalizes spacing around hyphens
    - removes spaces before ')', and after '(' 

    This aims to make labels stable enough for "most frequent label" mapping.
    """
    s = _as_str_series(s)
    s = s.fillna(pd.NA)

    # Replace newlines/tabs with space
    s = s.str.replace(r"[\r\n\t]+", " ", regex=True)

    # Normalize hyphen spacing: "A- B" / "A -B" / "A-B" -> "A - B"
    s = s.str.replace(r"\s*-\s*", " - ", regex=True)

    # Normalize parentheses spacing
    s = s.str.replace(r"\(\s+", "(", regex=True)
    s = s.str.replace(r"\s+\)", ")", regex=True)

    # Collapse multiple spaces
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()

    # Treat empty strings as NA
    s = s.replace({"": pd.NA})
    return s


def safe_div(numer: pd.Series | float, denom: pd.Series | float) -> pd.Series | float:
    """Safe division that returns NaN where denom is 0 or NaN."""
    return np.where((pd.isna(denom)) | (denom == 0), np.nan, numer / denom)


def mode_clean_label(df: pd.DataFrame, id_col: str, label_col: str) -> pd.DataFrame:
    """Create a stable mapping id -> most frequent cleaned label."""
    if id_col not in df.columns or label_col not in df.columns:
        return pd.DataFrame(columns=[id_col, f"{label_col}_clean"])  # empty

    tmp = df[[id_col, label_col]].copy()
    tmp[id_col] = _as_str_series(tmp[id_col]).str.strip()
    tmp[label_col] = clean_display_text(tmp[label_col])
    tmp = tmp.dropna(subset=[id_col, label_col])
    if tmp.empty:
        return pd.DataFrame(columns=[id_col, f"{label_col}_clean"])

    # Compute most frequent label per ID
    vc = (
        tmp.groupby(id_col)[label_col]
        .agg(lambda x: x.value_counts(dropna=True).index[0])
        .reset_index()
        .rename(columns={label_col: f"{label_col}_clean"})
    )
    return vc


def version_label(v: str) -> str:
    """Sheet-safe version label."""
    v = (v or "").strip().lower()
    if v == "3.15.46":
        return "11_4"
    if v == "12.2.20":
        return "12_2"
    # fallback: keep digits and underscores
    return re.sub(r"[^0-9a-z]+", "_", v).strip("_")[:20]


@dataclass
class TripFilterQC:
    total_rows: int
    excluded_short_mobile: int
    excluded_negative_mobile: int
    excluded_extreme_long_lowdist: int
    kept_rows: int


# -----------------------------
# Loading & normalization
# -----------------------------

def load_events(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    # Normalize key columns if present
    for col in ["software_version", "guardian_unit", "event_type", "classification"]:
        if col in df.columns:
            df[col] = norm_token(df[col])

    # Parse timestamps
    if "detection_time" in df.columns:
        df["detection_time"] = pd.to_datetime(df["detection_time"], utc=True, errors="coerce")

    # IDs and display names
    for col in ["account_id", "fleet_id"]:
        if col in df.columns:
            df[col] = _as_str_series(df[col]).str.strip()

    for col in ["account", "fleet"]:
        if col in df.columns:
            df[col] = clean_display_text(df[col])

    return df


def load_trips(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    # Normalize key columns if present
    for col in ["software_version", "guardian_unit"]:
        if col in df.columns:
            df[col] = norm_token(df[col])

    # Parse timestamps
    if "start_time" in df.columns:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")

    # Numeric coercions
    for col in ["mobile_mins", "operating_mins", "distance_kms"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # IDs and display names
    for col in ["account_id", "fleet_id"]:
        if col in df.columns:
            df[col] = _as_str_series(df[col]).str.strip()

    for col in ["account", "fleet"]:
        if col in df.columns:
            df[col] = clean_display_text(df[col])

    return df


# -----------------------------
# Trip filtering & exposure
# -----------------------------

def apply_trip_filters(trips: pd.DataFrame) -> Tuple[pd.DataFrame, TripFilterQC, pd.DataFrame]:
    """Apply exposure filtering rules to trips.

    Returns:
      - filtered trips (kept)
      - QC counts
      - trip_rows_with_flags (for debugging if desired)
    """
    required = {"mobile_mins", "distance_kms"}
    missing = [c for c in required if c not in trips.columns]
    if missing:
        raise ValueError(f"Trips file missing required columns: {missing}")

    t = trips.copy()

    short_mobile = t["mobile_mins"] < TRIP_RULE_MIN_MOBILE_MINS
    negative_mobile = t["mobile_mins"] < 0
    extreme_long_lowdist = (t["mobile_mins"] > TRIP_RULE_EXTREME_MOBILE_MINS) & (t["distance_kms"] < TRIP_RULE_EXTREME_MAX_DISTANCE_KMS)

    exclude_any = short_mobile | negative_mobile | extreme_long_lowdist

    qc = TripFilterQC(
        total_rows=len(t),
        excluded_short_mobile=int(short_mobile.sum(skipna=True)),
        excluded_negative_mobile=int(negative_mobile.sum(skipna=True)),
        excluded_extreme_long_lowdist=int(extreme_long_lowdist.sum(skipna=True)),
        kept_rows=int((~exclude_any).sum(skipna=True)),
    )

    t_flags = t.assign(
        exclude_short_mobile=short_mobile,
        exclude_negative_mobile=negative_mobile,
        exclude_extreme_long_lowdist=extreme_long_lowdist,
        exclude_any=exclude_any,
    )

    kept = t_flags.loc[~exclude_any].copy()
    return kept, qc, t_flags


def compute_exposure(trips_kept: pd.DataFrame) -> pd.DataFrame:
    """Compute exposure hours per (guardian_unit, software_version)."""
    required = {"guardian_unit", "software_version", "mobile_mins", "operating_mins"}
    missing = [c for c in required if c not in trips_kept.columns]
    if missing:
        raise ValueError(f"Trips file missing required columns after load: {missing}")

    t = trips_kept.copy()
    t["mobile_hours"] = t["mobile_mins"] / 60.0
    t["operating_hours"] = t["operating_mins"] / 60.0

    exp = (
        t.groupby(["guardian_unit", "software_version"], dropna=False)
        .agg(
            mobile_hours=("mobile_hours", "sum"),
            operating_hours=("operating_hours", "sum"),
            trips_count=("mobile_mins", "size"),
        )
        .reset_index()
    )
    return exp


def determine_included_units(exposure: pd.DataFrame, versions: Sequence[str], min_hours: float) -> pd.DataFrame:
    """Return a DataFrame of units that meet min mobile hours in BOTH versions."""
    exp = exposure.copy()
    exp = exp[exp["software_version"].isin([v.lower() for v in versions])].copy()

    # pivot mobile_hours by version
    piv = exp.pivot_table(index="guardian_unit", columns="software_version", values="mobile_hours", aggfunc="sum")

    # Ensure both versions exist as columns (even if all-NaN)
    for v in [v.lower() for v in versions]:
        if v not in piv.columns:
            piv[v] = np.nan

    ok = (piv[[v.lower() for v in versions]] >= min_hours).all(axis=1)
    included = piv.loc[ok].reset_index()

    # Keep exposure columns for both versions
    included = included.rename(columns={v.lower(): f"mobile_hours_{version_label(v.lower())}" for v in versions})
    return included


def exposure_totals_for_included(exposure: pd.DataFrame, included_units: pd.DataFrame, versions: Sequence[str]) -> pd.DataFrame:
    inc = set(included_units["guardian_unit"].dropna().tolist())
    exp = exposure[exposure["guardian_unit"].isin(inc)].copy()
    exp = exp[exp["software_version"].isin([v.lower() for v in versions])].copy()

    totals = (
        exp.groupby("software_version")
        .agg(
            included_units=("guardian_unit", "nunique"),
            total_mobile_hours=("mobile_hours", "sum"),
            total_operating_hours=("operating_hours", "sum"),
        )
        .reset_index()
    )
    return totals


# -----------------------------
# Event metrics
# -----------------------------

def _prep_events_for_metrics(events: pd.DataFrame, versions: Sequence[str], included_units: pd.DataFrame) -> pd.DataFrame:
    e = events.copy()

    required = {"software_version", "guardian_unit", "event_type", "classification"}
    missing = [c for c in required if c not in e.columns]
    if missing:
        raise ValueError(f"Events file missing required columns: {missing}")

    e = e[e["software_version"].isin([v.lower() for v in versions])].copy()

    inc = set(included_units["guardian_unit"].dropna().tolist())
    e = e[e["guardian_unit"].isin(inc)].copy()

    return e


def compute_pooled_metrics(
    events: pd.DataFrame,
    exposure_totals: pd.DataFrame,
    versions: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute pooled metrics per version.

    Returns three tables:
      - microsleep base
      - microsleep + yawning
      - lga
    """
    e = events.copy()

    # Build quick lookup of mobile hours per version
    exp = exposure_totals.set_index("software_version")
    mobile_hours = exp["total_mobile_hours"].to_dict()

    def pooled_for_mask(mask: pd.Series, tp_mask: pd.Series) -> pd.DataFrame:
        sub = e.loc[mask].copy()
        if sub.empty:
            rows = []
            for v in [v.lower() for v in versions]:
                rows.append(
                    dict(
                        software_version=v,
                        event_count=0,
                        tp_count=0,
                        fp_count=0,
                        precision=np.nan,
                        events_per_mobile_hour=np.nan,
                        tp_per_mobile_hour=np.nan,
                        fp_per_mobile_hour=np.nan,
                    )
                )
            return pd.DataFrame(rows)

        sub["is_tp"] = tp_mask.loc[sub.index].astype(bool)
        sub["is_fp"] = ~sub["is_tp"]

        agg = (
            sub.groupby("software_version")
            .agg(
                event_count=("is_tp", "size"),
                tp_count=("is_tp", "sum"),
                fp_count=("is_fp", "sum"),
            )
            .reset_index()
        )

        # Ensure both versions present
        allv = pd.DataFrame({"software_version": [v.lower() for v in versions]})
        agg = allv.merge(agg, on="software_version", how="left").fillna({"event_count": 0, "tp_count": 0, "fp_count": 0})

        agg["precision"] = safe_div(agg["tp_count"], agg["event_count"])  # event_count == TP+FP by construction
        agg["mobile_hours"] = agg["software_version"].map(mobile_hours)

        agg["events_per_mobile_hour"] = safe_div(agg["event_count"], agg["mobile_hours"])
        agg["tp_per_mobile_hour"] = safe_div(agg["tp_count"], agg["mobile_hours"])
        agg["fp_per_mobile_hour"] = safe_div(agg["fp_count"], agg["mobile_hours"])

        # Reorder columns
        cols = [
            "software_version",
            "mobile_hours",
            "event_count",
            "tp_count",
            "fp_count",
            "precision",
            "events_per_mobile_hour",
            "tp_per_mobile_hour",
            "fp_per_mobile_hour",
        ]
        return agg[cols]

    # Microsleep base
    ms_mask = e["event_type"] == EV_MS
    ms_tp_base = e["classification"].isin(MS_TP_BASE)
    pooled_ms_base = pooled_for_mask(ms_mask, ms_tp_base)

    # Microsleep + yawning
    ms_tp_yawn = e["classification"].isin(MS_TP_YAWN)
    pooled_ms_yawn = pooled_for_mask(ms_mask, ms_tp_yawn)

    # LGA
    lga_mask = e["event_type"] == EV_LGA
    lga_tp = e["classification"].isin(LGA_TP)
    pooled_lga = pooled_for_mask(lga_mask, lga_tp)

    return pooled_ms_base, pooled_ms_yawn, pooled_lga


def compute_unit_level_rates(
    events: pd.DataFrame,
    exposure: pd.DataFrame,
    versions: Sequence[str],
    variant: str,
    topn: int,
    id_maps: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """Compute per-version Top-N units by FP/hr.

    variant:
      - "ms_base"
      - "ms_yawn"
      - "lga"

    Returns dict: {version -> topN dataframe}
    """
    e = events.copy()

    if variant == "ms_base":
        mask = e["event_type"] == EV_MS
        is_tp = e["classification"].isin(MS_TP_BASE)
    elif variant == "ms_yawn":
        mask = e["event_type"] == EV_MS
        is_tp = e["classification"].isin(MS_TP_YAWN)
    elif variant == "lga":
        mask = e["event_type"] == EV_LGA
        is_tp = e["classification"].isin(LGA_TP)
    else:
        raise ValueError(f"Unknown variant: {variant}")

    sub = e.loc[mask].copy()
    sub["tp"] = is_tp.loc[sub.index].astype(int)
    sub["fp"] = (1 - sub["tp"]).astype(int)

    counts = (
        sub.groupby(["guardian_unit", "software_version"])
        .agg(tp_count=("tp", "sum"), fp_count=("fp", "sum"), event_count=("tp", "size"))
        .reset_index()
    )

    exp = exposure.copy()
    exp = exp[exp["software_version"].isin([v.lower() for v in versions])].copy()

    merged = counts.merge(exp, on=["guardian_unit", "software_version"], how="left")

    merged["tp_per_mobile_hour"] = safe_div(merged["tp_count"], merged["mobile_hours"])
    merged["fp_per_mobile_hour"] = safe_div(merged["fp_count"], merged["mobile_hours"])

    # Attach account/fleet context if possible: choose the most common account_id/fleet_id per unit+version from trips (exposure source)
    for key in ["account_id", "fleet_id", "account", "fleet"]:
        if key in exp.columns:
            # pick most frequent non-null value per unit+version
            tmp = (
                exp[["guardian_unit", "software_version", key]]
                .dropna(subset=[key])
                .groupby(["guardian_unit", "software_version"])[key]
                .agg(lambda x: x.value_counts(dropna=True).index[0])
                .reset_index()
            )
            merged = merged.merge(tmp, on=["guardian_unit", "software_version"], how="left")

    # Map cleaned account/fleet names from IDs (most frequent label by ID)
    if "account_id" in merged.columns and "account_id" in id_maps:
        merged = merged.merge(id_maps["account_id"], on="account_id", how="left")
    if "fleet_id" in merged.columns and "fleet_id" in id_maps:
        merged = merged.merge(id_maps["fleet_id"], on="fleet_id", how="left")

    # Build per-version topN
    out: Dict[str, pd.DataFrame] = {}
    for v in [v.lower() for v in versions]:
        dfv = merged[merged["software_version"] == v].copy()
        dfv = dfv.sort_values(["fp_per_mobile_hour", "mobile_hours"], ascending=[False, False])
        dfv = dfv.head(topn)

        cols = [
            "guardian_unit",
            "software_version",
            "fp_per_mobile_hour",
            "tp_per_mobile_hour",
            "fp_count",
            "tp_count",
            "event_count",
            "mobile_hours",
            "operating_hours",
        ]

        # Optional context columns
        for c in ["account_id", "account_clean", "fleet_id", "fleet_clean", "account", "fleet"]:
            if c in dfv.columns and c not in cols:
                cols.append(c)

        out[v] = dfv[cols]

    return out


# -----------------------------
# Daily breakdown
# -----------------------------

def compute_daily_tables(
    events_all: pd.DataFrame,
    trips_kept: pd.DataFrame,
    included_units: pd.DataFrame,
    versions: Sequence[str],
    mode: str,
) -> Dict[str, pd.DataFrame]:
    """Create daily tables with columns = date, rows = key metrics.

    mode:
      - "pooled": daily pooled metrics (ALL units) but using the SAME trip filters for exposure.
      - "thresholded": daily pooled metrics restricted to included units.

    Returns dict of sheet_name -> dataframe
    """
    mode = (mode or "none").lower()
    if mode not in {"pooled", "thresholded"}:
        return {}

    # Filter versions
    e = events_all.copy()
    e = e[e["software_version"].isin([v.lower() for v in versions])].copy()

    t = trips_kept.copy()
    t = t[t["software_version"].isin([v.lower() for v in versions])].copy()

    if mode == "thresholded":
        inc = set(included_units["guardian_unit"].dropna().tolist())
        e = e[e["guardian_unit"].isin(inc)].copy()
        t = t[t["guardian_unit"].isin(inc)].copy()

    # Daily exposure from trips (mobile hours)
    if "start_time" in t.columns:
        t["day"] = t["start_time"].dt.floor("D").dt.date
    else:
        t["day"] = pd.NaT

    t["mobile_hours"] = t["mobile_mins"] / 60.0
    t["operating_hours"] = t["operating_mins"] / 60.0

    exp_day = (
        t.groupby(["software_version", "day"])
        .agg(mobile_hours=("mobile_hours", "sum"), operating_hours=("operating_hours", "sum"))
        .reset_index()
    )

    # Daily event counts
    if "detection_time" in e.columns:
        e["day"] = e["detection_time"].dt.floor("D").dt.date
    else:
        e["day"] = pd.NaT

    def daily_metrics_for(event_type: str, tp_set: set, prefix: str) -> pd.DataFrame:
        sub = e[e["event_type"] == event_type].copy()
        sub["tp"] = sub["classification"].isin(tp_set).astype(int)
        sub["fp"] = (1 - sub["tp"]).astype(int)

        agg = (
            sub.groupby(["software_version", "day"])
            .agg(event_count=("tp", "size"), tp_count=("tp", "sum"), fp_count=("fp", "sum"))
            .reset_index()
        )

        # Merge exposure
        m = agg.merge(exp_day, on=["software_version", "day"], how="left")

        # Rates
        m["events_per_mobile_hour"] = safe_div(m["event_count"], m["mobile_hours"])
        m["tp_per_mobile_hour"] = safe_div(m["tp_count"], m["mobile_hours"])
        m["fp_per_mobile_hour"] = safe_div(m["fp_count"], m["mobile_hours"])

        # Pivot into "day columns" format with rows = metrics by version
        m["day_str"] = m["day"].astype(str)

        rows = []
        for v in [v.lower() for v in versions]:
            mv = m[m["software_version"] == v].copy()
            if mv.empty:
                continue
            for metric in [
                "mobile_hours",
                "operating_hours",
                "event_count",
                "tp_count",
                "fp_count",
                "events_per_mobile_hour",
                "tp_per_mobile_hour",
                "fp_per_mobile_hour",
            ]:
                tmp = mv.pivot_table(index=None, columns="day_str", values=metric, aggfunc="sum")
                tmp.index = [f"{v} | {prefix} | {metric}"]
                rows.append(tmp)

        if not rows:
            return pd.DataFrame()

        out = pd.concat(rows, axis=0).reset_index().rename(columns={"index": "metric"})
        return out

    sheets: Dict[str, pd.DataFrame] = {}

    sheets[f"Daily_MS_base_{mode}"] = daily_metrics_for(EV_MS, MS_TP_BASE, "MS_base")
    sheets[f"Daily_MS_yawn_{mode}"] = daily_metrics_for(EV_MS, MS_TP_YAWN, "MS_yawn")
    sheets[f"Daily_LGA_{mode}"] = daily_metrics_for(EV_LGA, LGA_TP, "LGA")

    # VATS daily detections (no TP/FP) if present
    sub_v = e[e["event_type"] == EV_VATS].copy()
    if not sub_v.empty:
        agg_v = (
            sub_v.groupby(["software_version", "day"])
            .size()
            .reset_index(name="vats_events")
            .merge(exp_day, on=["software_version", "day"], how="left")
        )
        agg_v["vats_events_per_mobile_hour"] = safe_div(agg_v["vats_events"], agg_v["mobile_hours"])
        agg_v["day_str"] = agg_v["day"].astype(str)

        rows = []
        for v in [v.lower() for v in versions]:
            mv = agg_v[agg_v["software_version"] == v].copy()
            if mv.empty:
                continue
            for metric in ["mobile_hours", "operating_hours", "vats_events", "vats_events_per_mobile_hour"]:
                tmp = mv.pivot_table(index=None, columns="day_str", values=metric, aggfunc="sum")
                tmp.index = [f"{v} | VATS | {metric}"]
                rows.append(tmp)
        if rows:
            sheets[f"Daily_VATS_{mode}"] = pd.concat(rows, axis=0).reset_index().rename(columns={"index": "metric"})

    # Remove empties
    sheets = {k: v for k, v in sheets.items() if isinstance(v, pd.DataFrame) and not v.empty}
    return sheets


# -----------------------------
# Excel export
# -----------------------------

def build_readme_sheet(
    events_path: str,
    trips_path: str,
    versions: Sequence[str],
    min_hours: float,
    qc: TripFilterQC,
    daily_mode: str,
) -> pd.DataFrame:
    lines = [
        "Repeatable 11.4 vs 12.2 performance workbook",
        "",
        f"Events file: {events_path}",
        f"Trips file:  {trips_path}",
        "",
        "Key definitions",
        "- Rates are per MOBILE hour (alerts can’t fire when stationary).",
        "- Trip filtering applies ONLY to exposure (denominator), not event numerators.",
        f"- Units included in pooled metrics/rankings must have >= {min_hours:g} mobile hours in BOTH versions: {', '.join(versions)}.",
        "",
        "Trip exposure filters",
        f"- Exclude trips with mobile_mins < {TRIP_RULE_MIN_MOBILE_MINS}",
        "- Exclude trips with mobile_mins < 0",
        f"- Exclude trips where mobile_mins > {TRIP_RULE_EXTREME_MOBILE_MINS} AND distance_kms < {TRIP_RULE_EXTREME_MAX_DISTANCE_KMS}",
        "",
        "Trip filter QC (this run)",
        f"- Trips total rows: {qc.total_rows}",
        f"- Excluded short mobile: {qc.excluded_short_mobile}",
        f"- Excluded negative mobile: {qc.excluded_negative_mobile}",
        f"- Excluded extreme long+low dist: {qc.excluded_extreme_long_lowdist}",
        f"- Kept trips for exposure: {qc.kept_rows}",
        "",
        "TP/FP definitions",
        "Microsleep (Variant A / base): TP if classification in {microsleep, drowsiness}; FP otherwise.",
        "Microsleep (Variant B / +yawning): TP if classification in {microsleep, drowsiness, yawning}; FP otherwise.",
        "LGA: TP if classification in {long glance away, mobile device, other distraction}; FP otherwise.",
        "",
        "Daily tables",
        f"- daily_mode = {daily_mode} (pooled = all units, thresholded = included units only)",
    ]
    return pd.DataFrame({"README": lines})


def export_to_excel(
    out_path: str,
    readme_df: pd.DataFrame,
    exposure_summary: pd.DataFrame,
    pooled_ms_base: pd.DataFrame,
    pooled_ms_yawn: pd.DataFrame,
    pooled_lga: pd.DataFrame,
    included_units: pd.DataFrame,
    top_ms_base: Dict[str, pd.DataFrame],
    top_lga: Dict[str, pd.DataFrame],
    top_ms_yawn: Optional[Dict[str, pd.DataFrame]] = None,
    daily_sheets: Optional[Dict[str, pd.DataFrame]] = None,
) -> None:
    daily_sheets = daily_sheets or {}

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        # README first
        readme_df.to_excel(xw, sheet_name="README", index=False)

        exposure_summary.to_excel(xw, sheet_name="Exposure_Summary", index=False)
        pooled_ms_base.to_excel(xw, sheet_name="Pooled_MS_base", index=False)
        pooled_ms_yawn.to_excel(xw, sheet_name="Pooled_MS_yawn", index=False)
        pooled_lga.to_excel(xw, sheet_name="Pooled_LGA", index=False)

        included_units.to_excel(xw, sheet_name="Included_Units_List", index=False)

        # Top lists per version
        for v, dfv in top_ms_base.items():
            sheet = f"Top{len(dfv)}_FPhr_MS_base_{version_label(v)}"[:31]
            dfv.to_excel(xw, sheet_name=sheet, index=False)

        if top_ms_yawn:
            for v, dfv in top_ms_yawn.items():
                sheet = f"Top{len(dfv)}_FPhr_MS_yawn_{version_label(v)}"[:31]
                dfv.to_excel(xw, sheet_name=sheet, index=False)

        for v, dfv in top_lga.items():
            sheet = f"Top{len(dfv)}_FPhr_LGA_{version_label(v)}"[:31]
            dfv.to_excel(xw, sheet_name=sheet, index=False)

        # Daily sheets
        for name, df in daily_sheets.items():
            xw_sheet = name[:31]
            df.to_excel(xw, sheet_name=xw_sheet, index=False)


# -----------------------------
# Main / orchestration
# -----------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Repeatable 11.4 vs 12.2 performance comparison (events + trips CSVs)")
    p.add_argument("--events", required=True, help="Path to events CSV")
    p.add_argument("--trips", required=True, help="Path to trips CSV")
    p.add_argument("--out", required=True, help="Output Excel workbook (.xlsx)")

    p.add_argument("--versions", default=",".join(DEFAULT_VERSIONS), help="Comma-separated versions to compare")
    p.add_argument("--min-hours", type=float, default=DEFAULT_MIN_HOURS, help="Min MOBILE hours per unit per version (must meet in BOTH versions)")
    p.add_argument("--topn", type=int, default=DEFAULT_TOPN, help="Top N units to list for worst FP/hr")

    p.add_argument(
        "--daily",
        default="none",
        choices=["none", "pooled", "thresholded"],
        help="Daily tables: none | pooled (all units) | thresholded (included units only)",
    )

    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    if len(versions) != 2:
        print("WARNING: This script is designed for 2 versions; continuing anyway.", file=sys.stderr)

    # Load
    events_all = load_events(args.events)
    trips_all = load_trips(args.trips)

    print(f"Loaded events rows: {len(events_all):,}")
    print(f"Loaded trips rows:  {len(trips_all):,}")

    # Name maps (most frequent cleaned label by ID) across both files
    combined_names = pd.concat(
        [
            events_all[[c for c in ["account_id", "account", "fleet_id", "fleet"] if c in events_all.columns]].copy(),
            trips_all[[c for c in ["account_id", "account", "fleet_id", "fleet"] if c in trips_all.columns]].copy(),
        ],
        axis=0,
        ignore_index=True,
    )

    id_maps: Dict[str, pd.DataFrame] = {}
    if "account_id" in combined_names.columns and "account" in combined_names.columns:
        id_maps["account_id"] = mode_clean_label(combined_names, "account_id", "account")
    if "fleet_id" in combined_names.columns and "fleet" in combined_names.columns:
        id_maps["fleet_id"] = mode_clean_label(combined_names, "fleet_id", "fleet")

    # Trip filtering for exposure
    trips_kept, qc, _ = apply_trip_filters(trips_all)
    print("Trip exclusion QC:")
    print(f"  total_rows                 : {qc.total_rows:,}")
    print(f"  excluded_short_mobile (<{TRIP_RULE_MIN_MOBILE_MINS}): {qc.excluded_short_mobile:,}")
    print(f"  excluded_negative_mobile   : {qc.excluded_negative_mobile:,}")
    print(f"  excluded_extreme_long_lowdist: {qc.excluded_extreme_long_lowdist:,}")
    print(f"  kept_rows                  : {qc.kept_rows:,}")

    # Exposure per unit+version
    exposure = compute_exposure(trips_kept)

    # Included units list
    included_units = determine_included_units(exposure, versions=versions, min_hours=args.min_hours)
    print(f"Included units meeting >= {args.min_hours:g} mobile hours in BOTH versions: {len(included_units):,}")

    # Exposure totals for included units (pooled denominators)
    exposure_totals = exposure_totals_for_included(exposure, included_units, versions=versions)
    print("Exposure totals (included units only):")
    print(exposure_totals.to_string(index=False))

    # Prepare events restricted to included units
    events_inc = _prep_events_for_metrics(events_all, versions=versions, included_units=included_units)
    print(f"Events rows for included units (both versions): {len(events_inc):,}")

    # Pooled metrics
    pooled_ms_base, pooled_ms_yawn, pooled_lga = compute_pooled_metrics(events_inc, exposure_totals, versions=versions)

    # Unit-level top lists
    top_ms_base = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="ms_base", topn=args.topn, id_maps=id_maps)
    top_ms_yawn = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="ms_yawn", topn=args.topn, id_maps=id_maps)
    top_lga = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="lga", topn=args.topn, id_maps=id_maps)

    # Daily breakdown
    daily_sheets = compute_daily_tables(
        events_all=events_all,
        trips_kept=trips_kept,
        included_units=included_units,
        versions=versions,
        mode=args.daily,
    )

    # README
    readme_df = build_readme_sheet(
        events_path=args.events,
        trips_path=args.trips,
        versions=versions,
        min_hours=args.min_hours,
        qc=qc,
        daily_mode=args.daily,
    )

    # Export
    export_to_excel(
        out_path=args.out,
        readme_df=readme_df,
        exposure_summary=exposure_totals,
        pooled_ms_base=pooled_ms_base,
        pooled_ms_yawn=pooled_ms_yawn,
        pooled_lga=pooled_lga,
        included_units=included_units,
        top_ms_base=top_ms_base,
        top_ms_yawn=top_ms_yawn,
        top_lga=top_lga,
        daily_sheets=daily_sheets,
    )

    print(f"Wrote Excel workbook: {args.out}")
    if daily_sheets:
        print(f"Daily sheets written: {', '.join(sorted(daily_sheets.keys()))}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
