"""app.py — Streamlit GUI for the SW Version Performance Comparator.

Run with:
    python -m streamlit run app.py
"""

import io
import os
import tempfile
import traceback

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="SW Version Comparator",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Import core logic
# ---------------------------------------------------------------------------
_import_error = ""
try:
    from run_sw_compare import (
        DEFAULT_MIN_HOURS,
        DEFAULT_TOPN,
        DEFAULT_VERSIONS,
        EV_MS,
        EV_LGA,
        MS_TP_BASE,
        MS_TP_YAWN,
        LGA_TP,
        TRIP_RULE_EXTREME_MAX_DISTANCE_KMS,
        TRIP_RULE_EXTREME_MOBILE_MINS,
        TRIP_RULE_MIN_MOBILE_MINS,
        apply_trip_filters,
        build_readme_sheet,
        compute_daily_tables,
        compute_exposure,
        compute_pooled_metrics,
        compute_unit_level_rates,
        determine_included_units,
        export_to_excel,
        exposure_totals_for_included,
        load_events,
        load_trips,
        mode_clean_label,
        _prep_events_for_metrics,
    )
    _import_ok = True
except Exception as _e:
    _import_ok = False
    _import_error = str(_e)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EVENTS_REQUIRED_COLS = ["software_version", "guardian_unit", "event_type", "classification"]
TRIPS_REQUIRED_COLS  = ["software_version", "guardian_unit", "mobile_mins", "operating_mins", "distance_kms"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def save_upload_to_tmp(uploaded_file) -> str:
    suffix = os.path.splitext(uploaded_file.name)[-1] or ".csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp.flush()
    tmp.close()
    return tmp.name


def fmt_num(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def validate_csv_columns(df: pd.DataFrame, required: list, label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"**{label}** is missing required column(s): {', '.join(f'`{c}`' for c in missing)}\n\n"
            f"Columns found: {', '.join(df.columns.tolist())}"
        )


def make_comparison_df(pooled_df: pd.DataFrame) -> pd.DataFrame:
    """Reshape a 2-row pooled metrics df into a Metric | vA | vB | Δ(B−A) table."""
    metric_cols = [m for m in [
        "mobile_hours", "event_count", "tp_count", "fp_count",
        "precision", "events_per_mobile_hour", "tp_per_mobile_hour", "fp_per_mobile_hour",
    ] if m in pooled_df.columns]

    if len(pooled_df) < 2:
        return pooled_df

    v1_row  = pooled_df.iloc[0]
    v2_row  = pooled_df.iloc[1]
    v1_name = str(v1_row.get("software_version", "Version A"))
    v2_name = str(v2_row.get("software_version", "Version B"))

    rows = []
    for m in metric_cols:
        try:
            delta = float(v2_row[m]) - float(v1_row[m])
        except Exception:
            delta = None
        rows.append({"Metric": m, v1_name: v1_row[m], v2_name: v2_row[m], "Δ (B − A)": delta})

    return pd.DataFrame(rows)


def make_bar_chart_df(pooled_df: pd.DataFrame) -> pd.DataFrame:
    """Return a df suitable for st.bar_chart showing rates per mobile hour by version."""
    rate_metrics = [m for m in ["fp_per_mobile_hour", "tp_per_mobile_hour", "events_per_mobile_hour"]
                    if m in pooled_df.columns]
    chart: dict = {}
    for _, row in pooled_df.iterrows():
        v = str(row.get("software_version", "?"))
        for m in rate_metrics:
            chart.setdefault(m, {})[v] = row[m]
    return pd.DataFrame(chart).T


def compute_account_breakdown(
    events_inc: pd.DataFrame,
    trips_kept: pd.DataFrame,
    versions: list,
    event_type_val: str,
    tp_classifications: set,
) -> pd.DataFrame:
    """Compute per-account FP/hr, TP/hr, precision for each version side-by-side with delta."""
    account_col = "account"
    t = trips_kept.copy()
    t["software_version"] = t["software_version"].str.strip().str.lower()
    t["mobile_hours"] = t["mobile_mins"] / 60.0

    if account_col not in t.columns:
        return pd.DataFrame()

    t = t[t["software_version"].isin([v.lower() for v in versions])]
    exp = (
        t.groupby([account_col, "software_version"])
        .agg(mobile_hours=("mobile_hours", "sum"))
        .reset_index()
    )

    e = events_inc.copy()
    e["software_version"] = e["software_version"].str.strip().str.lower()
    if "event_type" not in e.columns or account_col not in e.columns:
        return pd.DataFrame()

    sub = e[e["event_type"] == event_type_val].copy()
    sub["is_tp"] = sub["classification"].isin(tp_classifications)
    sub["is_fp"] = ~sub["is_tp"]

    ev_agg = (
        sub.groupby([account_col, "software_version"])
        .agg(event_count=("is_tp", "size"), tp_count=("is_tp", "sum"), fp_count=("is_fp", "sum"))
        .reset_index()
    )

    merged = exp.merge(ev_agg, on=[account_col, "software_version"], how="left").fillna(
        {"event_count": 0, "tp_count": 0, "fp_count": 0}
    )
    merged["fp_per_hr"] = merged["fp_count"] / merged["mobile_hours"].replace(0, float("nan"))
    merged["tp_per_hr"] = merged["tp_count"] / merged["mobile_hours"].replace(0, float("nan"))
    merged["precision"] = merged["tp_count"] / merged["event_count"].replace(0, float("nan"))

    v_lo = [v.lower() for v in versions]
    va, vb = v_lo[0], v_lo[1]

    def pivot_metric(metric: str, delta: bool = False) -> pd.DataFrame:
        piv = merged.pivot_table(index=account_col, columns="software_version", values=metric)
        for v in v_lo:
            if v not in piv.columns:
                piv[v] = float("nan")
        piv = piv[[va, vb]].copy()
        piv.columns = [f"{metric} ({va})", f"{metric} ({vb})"]
        if delta:
            piv[f"{metric} delta (B-A)"] = piv[f"{metric} ({vb})"] - piv[f"{metric} ({va})"]
        return piv

    frames = (
        [pivot_metric(m, delta=False) for m in ["mobile_hours", "event_count", "tp_count", "fp_count"]] +
        [pivot_metric(m, delta=True)  for m in ["fp_per_hr", "tp_per_hr", "precision"]]
    )
    result = pd.concat(frames, axis=1).reset_index()
    return result.sort_values(f"fp_per_hr ({vb})", ascending=False, na_position="last")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("📊 Software Version Performance Comparator")
st.caption("Compare Guardian software versions using Events & Trips CSV exports.")

if not _import_ok:
    st.error(
        f"❌ Could not import core logic from **run_sw_compare.py**: `{_import_error}`\n\n"
        "Make sure `run_sw_compare.py` is in the same folder as this app."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Settings")

    st.subheader("Software versions to compare")
    v1 = st.text_input("Version A", value=DEFAULT_VERSIONS[0], help="e.g. 3.15.46")
    v2 = st.text_input("Version B", value=DEFAULT_VERSIONS[1], help="e.g. 12.2.20")

    st.divider()
    st.subheader("Filtering")
    min_hours = st.number_input(
        "Min mobile hours per unit (in BOTH versions)",
        min_value=0.0, value=float(DEFAULT_MIN_HOURS), step=5.0,
        help="Units must have at least this many mobile hours in each version to be included.",
    )
    top_n = st.number_input(
        "Top N worst FP/hr units",
        min_value=1, max_value=100, value=DEFAULT_TOPN, step=1,
        help="How many of the worst-performing units to show in the rankings.",
    )

    st.divider()
    st.subheader("Daily breakdown")
    daily_mode = st.selectbox(
        "Daily tables",
        options=["none", "daily breakdown"],
        index=0,
        help="**none** – no daily sheets  \n**daily breakdown** – daily metrics for all units",
    )
    daily_mode_internal = "pooled" if daily_mode == "daily breakdown" else "none"

    st.caption(
        f"Built-in trip filter rules (always applied):  \n"
        f"• Exclude trips < {TRIP_RULE_MIN_MOBILE_MINS} mobile mins  \n"
        f"• Exclude negative mobile mins  \n"
        f"• Exclude trips > {TRIP_RULE_EXTREME_MOBILE_MINS} mins AND distance < {TRIP_RULE_EXTREME_MAX_DISTANCE_KMS} km"
    )

    st.divider()
    st.subheader("🔍 Event Filters")

    confirmed_only = st.checkbox(
        "Verified events only", value=False,
        help="Only include events with `confirmation = verified`.",
    )
    exclude_auto = st.checkbox(
        "Exclude auto-classified events", value=False,
        help="Exclude events tagged `auto-classified` (not human-reviewed).",
    )

    st.markdown("**Speed at event (kph)**")
    speed_min, speed_max = st.slider(
        "Speed range", min_value=0, max_value=250, value=(0, 250), step=5,
        help="Exclude events outside this speed range.",
    )

    st.markdown("**Event duration (seconds)**")
    dur_min, dur_max = st.slider(
        "Duration range", min_value=0, max_value=120, value=(0, 120), step=1,
        help="Exclude events outside this duration range.",
    )

    _ev_type_opts = sorted(st.session_state.get("_ev_type_opts", ["vats", "lga", "microsleep"]))
    selected_event_types = st.multiselect(
        "Event types to include", options=_ev_type_opts, default=_ev_type_opts,
        help="Deselect event types to exclude them from all metrics.",
    )

    _sp_opts = sorted(st.session_state.get("_sp_opts", []))
    selected_sps = st.multiselect(
        "Restrict to service providers (empty = all)", options=_sp_opts, default=[],
        help="Scope the comparison to specific service providers.",
    )

    _account_opts = sorted(st.session_state.get("_account_opts", []))
    selected_accounts = st.multiselect(
        "Restrict to accounts (empty = all)", options=_account_opts, default=[],
        help="Scope the comparison to specific accounts.",
    )

    st.divider()
    st.subheader("🛣️ Trip Exposure Filters")

    min_trip_distance = st.number_input(
        "Min trip distance (km)", min_value=0.0, value=0.0, step=0.5,
        help="Exclude trips shorter than this distance from the exposure denominator.",
    )
    max_avg_speed = st.number_input(
        "Max implied avg speed (kph)", min_value=50, max_value=300, value=200, step=10,
        help="Exclude trips where distance_kms / (mobile_mins/60) exceeds this value.",
    )

# ---------------------------------------------------------------------------
# File uploads
# ---------------------------------------------------------------------------
st.subheader("1️⃣  Upload your CSV files")
col_ev, col_tr = st.columns(2)
with col_ev:
    events_file = st.file_uploader("Events CSV", type=["csv"], help="Guardian Live events export CSV.")
with col_tr:
    trips_file = st.file_uploader("Trips CSV", type=["csv"], help="Guardian Live trips export CSV.")

st.subheader("2️⃣  Name your output file")
out_filename = st.text_input(
    "Output Excel filename", value="results_comparison.xlsx",
    help="Name of the Excel workbook to generate.",
)
if not out_filename.endswith(".xlsx"):
    out_filename += ".xlsx"

st.subheader("3️⃣  Run the comparison")
run_btn = st.button("▶  Run Comparison", type="primary", disabled=(events_file is None or trips_file is None))

if events_file is None or trips_file is None:
    st.info("👆 Please upload both CSV files to enable the Run button.")

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
if run_btn:
    versions = [v.strip() for v in [v1, v2] if v.strip()]
    if len(versions) != 2:
        st.error("Please enter exactly two software versions in the sidebar.")
        st.stop()

    progress = st.progress(0, text="Starting…")
    tmp_events = tmp_trips = None

    try:
        progress.progress(5,  text="Saving uploaded files…")
        tmp_events = save_upload_to_tmp(events_file)
        tmp_trips  = save_upload_to_tmp(trips_file)

        progress.progress(10, text="Loading Events CSV…")
        events_all = load_events(tmp_events)

        progress.progress(20, text="Loading Trips CSV…")
        trips_all = load_trips(tmp_trips)

        progress.progress(22, text="Validating CSV columns…")
        try:
            validate_csv_columns(events_all, EVENTS_REQUIRED_COLS, "Events CSV")
            validate_csv_columns(trips_all,  TRIPS_REQUIRED_COLS,  "Trips CSV")
        except RuntimeError as _ve:
            progress.empty()
            st.error(f"❌ Column validation failed:\n\n{_ve}")
            st.stop()

        # Populate sidebar dynamic options for next run
        if "event_type"       in events_all.columns:
            st.session_state["_ev_type_opts"] = sorted(events_all["event_type"].dropna().unique().tolist())
        if "service_provider" in events_all.columns:
            st.session_state["_sp_opts"]      = sorted(events_all["service_provider"].dropna().unique().tolist())
        if "account"          in events_all.columns:
            st.session_state["_account_opts"] = sorted(events_all["account"].dropna().unique().tolist())

        # Event-level filters
        progress.progress(25, text="Applying event filters…")
        ev_filter_log: list = []
        ev_total = len(events_all)

        if confirmed_only and "confirmation" in events_all.columns:
            before = len(events_all)
            events_all = events_all[events_all["confirmation"] == "verified"]
            ev_filter_log.append(("Verified only", before, len(events_all)))

        if exclude_auto and "tags" in events_all.columns:
            before = len(events_all)
            events_all = events_all[~events_all["tags"].fillna("").str.contains("auto-classified", case=False)]
            ev_filter_log.append(("Exclude auto-classified", before, len(events_all)))

        if (speed_min > 0 or speed_max < 250) and "speed_kph" in events_all.columns:
            before = len(events_all)
            spd = pd.to_numeric(events_all["speed_kph"], errors="coerce")
            events_all = events_all[(spd >= speed_min) & (spd <= speed_max)]
            ev_filter_log.append((f"Speed {speed_min}–{speed_max} kph", before, len(events_all)))

        if (dur_min > 0 or dur_max < 120) and "duration_seconds" in events_all.columns:
            before = len(events_all)
            dur = pd.to_numeric(events_all["duration_seconds"], errors="coerce")
            events_all = events_all[(dur >= dur_min) & (dur <= dur_max)]
            ev_filter_log.append((f"Duration {dur_min}–{dur_max}s", before, len(events_all)))

        if "event_type" in events_all.columns:
            all_ev_types = set(events_all["event_type"].dropna().unique())
            if selected_event_types and set(selected_event_types) != all_ev_types:
                before = len(events_all)
                events_all = events_all[events_all["event_type"].isin(selected_event_types)]
                ev_filter_log.append((f"Event types: {', '.join(selected_event_types)}", before, len(events_all)))

        if selected_sps and "service_provider" in events_all.columns:
            before = len(events_all)
            events_all = events_all[events_all["service_provider"].isin(selected_sps)]
            ev_filter_log.append((f"Service providers ({len(selected_sps)})", before, len(events_all)))

        if selected_accounts and "account" in events_all.columns:
            before = len(events_all)
            events_all = events_all[events_all["account"].isin(selected_accounts)]
            ev_filter_log.append((f"Accounts ({len(selected_accounts)})", before, len(events_all)))

        # Custom trip filters
        progress.progress(28, text="Applying trip filters…")
        tr_filter_log: list = []
        tr_total = len(trips_all)

        if min_trip_distance > 0 and "distance_kms" in trips_all.columns:
            before = len(trips_all)
            trips_all = trips_all[pd.to_numeric(trips_all["distance_kms"], errors="coerce") >= min_trip_distance]
            tr_filter_log.append((f"Min distance ≥ {min_trip_distance} km", before, len(trips_all)))

        if max_avg_speed < 200 and "distance_kms" in trips_all.columns and "mobile_mins" in trips_all.columns:
            before  = len(trips_all)
            dist    = pd.to_numeric(trips_all["distance_kms"], errors="coerce")
            mob_hr  = pd.to_numeric(trips_all["mobile_mins"],  errors="coerce") / 60.0
            trips_all = trips_all[dist / mob_hr.replace(0, float("nan")) <= max_avg_speed]
            tr_filter_log.append((f"Implied avg speed ≤ {max_avg_speed} kph", before, len(trips_all)))

        # Name maps
        progress.progress(32, text="Building name maps…")
        combined_names = pd.concat(
            [
                events_all[[c for c in ["account_id", "account", "fleet_id", "fleet"] if c in events_all.columns]].copy(),
                trips_all [[c for c in ["account_id", "account", "fleet_id", "fleet"] if c in trips_all.columns ]].copy(),
            ],
            ignore_index=True,
        )
        id_maps = {}
        if "account_id" in combined_names.columns and "account" in combined_names.columns:
            id_maps["account_id"] = mode_clean_label(combined_names, "account_id", "account")
        if "fleet_id" in combined_names.columns and "fleet" in combined_names.columns:
            id_maps["fleet_id"] = mode_clean_label(combined_names, "fleet_id", "fleet")

        # Built-in trip filtering
        progress.progress(38, text="Applying built-in trip exposure filters…")
        trips_kept, qc, _ = apply_trip_filters(trips_all)

        # Exposure
        progress.progress(48, text="Computing exposure (mobile hours)…")
        exposure = compute_exposure(trips_kept)

        # Included units
        progress.progress(55, text="Determining included units…")
        included_units  = determine_included_units(exposure, versions=versions, min_hours=min_hours)
        exposure_totals = exposure_totals_for_included(exposure, included_units, versions=versions)

        # Events for included units
        progress.progress(60, text="Filtering events to included units…")
        events_inc = _prep_events_for_metrics(events_all, versions=versions, included_units=included_units)

        # Pooled metrics
        progress.progress(65, text="Computing pooled metrics…")
        pooled_ms_base, pooled_ms_yawn, pooled_lga = compute_pooled_metrics(
            events_inc, exposure_totals, versions=versions
        )

        # Unit-level rankings
        progress.progress(72, text="Computing unit-level FP/hr rankings…")
        top_ms_base = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="ms_base", topn=top_n, id_maps=id_maps)
        top_ms_yawn = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="ms_yawn", topn=top_n, id_maps=id_maps)
        top_lga     = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="lga",     topn=top_n, id_maps=id_maps)

        # Daily breakdown
        progress.progress(80, text="Building daily breakdowns…")
        daily_sheets = compute_daily_tables(
            events_all=events_all,
            trips_kept=trips_kept,
            included_units=included_units,
            versions=versions,
            mode=daily_mode_internal,
        )

        # Export Excel
        progress.progress(88, text="Writing Excel workbook…")
        readme_df = build_readme_sheet(
            events_path=events_file.name,
            trips_path=trips_file.name,
            versions=versions,
            min_hours=min_hours,
            qc=qc,
            daily_mode=daily_mode_internal,
        )
        excel_buffer = io.BytesIO()
        export_to_excel(
            out_path=excel_buffer,
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
        excel_buffer.seek(0)
        progress.progress(100, text="✅ Done!")

        # -----------------------------------------------------------------------
        # Results
        # -----------------------------------------------------------------------
        st.success("✅ Comparison complete! Download your workbook below, or browse the results here.")
        st.download_button(
            label="⬇️  Download Excel Workbook",
            data=excel_buffer,
            file_name=out_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.divider()

        # Trip filter QC
        with st.expander("🔍 Trip Filter QC (built-in rules)", expanded=False):
            st.table(pd.DataFrame({
                "Metric": [
                    "Total trip rows loaded",
                    f"Excluded — short mobile (< {TRIP_RULE_MIN_MOBILE_MINS} mins)",
                    "Excluded — negative mobile mins",
                    "Excluded — extreme long + low distance",
                    "✅ Kept for exposure",
                ],
                "Count": [
                    fmt_num(qc.total_rows),
                    fmt_num(qc.excluded_short_mobile),
                    fmt_num(qc.excluded_negative_mobile),
                    fmt_num(qc.excluded_extreme_long_lowdist),
                    fmt_num(qc.kept_rows),
                ],
            }))

        # Custom filter impact
        if ev_filter_log or tr_filter_log:
            with st.expander("🔻 Custom Filter Impact", expanded=True):
                if ev_filter_log:
                    st.markdown("**Event filters applied**")
                    rows = [{"Filter": "All events loaded", "Before": ev_total, "After": ev_total, "Removed": 0, "% kept": "100%"}]
                    for lbl, before, after in ev_filter_log:
                        rows.append({"Filter": lbl, "Before": before, "After": after,
                                     "Removed": before - after, "% kept": f"{100*after/ev_total:.1f}%"})
                    st.dataframe(pd.DataFrame(rows).set_index("Filter"), use_container_width=True)
                if tr_filter_log:
                    st.markdown("**Trip exposure filters applied**")
                    rows = [{"Filter": "All trips loaded", "Before": tr_total, "After": tr_total, "Removed": 0, "% kept": "100%"}]
                    for lbl, before, after in tr_filter_log:
                        rows.append({"Filter": lbl, "Before": before, "After": after,
                                     "Removed": before - after, "% kept": f"{100*after/tr_total:.1f}%"})
                    st.dataframe(pd.DataFrame(rows).set_index("Filter"), use_container_width=True)

        # Exposure summary
        st.subheader("📦 Exposure Summary")
        st.caption(f"**{len(included_units):,}** units met the ≥ {min_hours:g} mobile-hour threshold in BOTH versions.")
        st.dataframe(exposure_totals.set_index("software_version"), use_container_width=True)

        # Pooled metrics
        st.subheader("📈 Pooled Metrics")
        tab_ms, tab_ms_y, tab_lga = st.tabs(["Microsleep (base)", "Microsleep + Yawning", "LGA"])

        def _render_pooled_tab(pooled_df: pd.DataFrame, caption: str) -> None:
            st.caption(caption)
            cmp = make_comparison_df(pooled_df)
            float_cols = [c for c in cmp.columns if c != "Metric"]
            st.dataframe(
                cmp.set_index("Metric").style.format({c: "{:.4f}" for c in float_cols}, na_rep="—"),
                use_container_width=True,
            )
            chart_df = make_bar_chart_df(pooled_df)
            if not chart_df.empty:
                st.caption("📊 Rates per mobile hour (FP / TP / Total)")
                st.bar_chart(chart_df, use_container_width=True)

        with tab_ms:
            _render_pooled_tab(pooled_ms_base, "TP = microsleep or drowsiness")
        with tab_ms_y:
            _render_pooled_tab(pooled_ms_yawn, "TP = microsleep, drowsiness, or yawning")
        with tab_lga:
            _render_pooled_tab(pooled_lga, "TP = long glance away, mobile device, or other distraction")

        # Top N unit rankings
        st.subheader(f"🏆 Top {top_n} Units by FP/hr")
        for variant_label, top_dict in [
            ("Microsleep (base)",    top_ms_base),
            ("Microsleep + Yawning", top_ms_yawn),
            ("LGA",                  top_lga),
        ]:
            with st.expander(variant_label, expanded=False):
                for v, dfv in top_dict.items():
                    st.markdown(f"**Version: `{v}`**")
                    rate_cols   = [c for c in dfv.columns if "per_mobile_hour" in c]
                    dfv_display = dfv.set_index("guardian_unit") if "guardian_unit" in dfv.columns else dfv
                    st.dataframe(
                        dfv_display.style.format({c: "{:.4f}" for c in rate_cols if c in dfv_display.columns}, na_rep="—"),
                        use_container_width=True,
                    )

        # Account-level breakdown
        st.subheader("🏢 Account-Level Breakdown")
        acc_tab_ms, acc_tab_ms_y, acc_tab_lga = st.tabs(["Microsleep (base)", "Microsleep + Yawning", "LGA"])

        def _render_account_tab(event_type_val: str, tp_set: set, caption: str) -> None:
            df = compute_account_breakdown(
                events_inc=events_inc,
                trips_kept=trips_kept,
                versions=versions,
                event_type_val=event_type_val,
                tp_classifications=tp_set,
            )
            st.caption(caption)
            if df.empty:
                st.info("No `account` column found in your CSVs.")
                return

            delta_cols    = [c for c in df.columns if "delta"   in c]
            fp_delta_cols = [c for c in delta_cols if "fp_per_hr"  in c]
            tp_delta_cols = [c for c in delta_cols if "tp_per_hr"  in c]
            pr_delta_cols = [c for c in delta_cols if "precision"  in c]

            def colour_delta(val, lower_is_better: bool = True) -> str:
                try:
                    v = float(val)
                    colour = ("#d4edda" if (v < 0) == lower_is_better else "#f8d7da") if v != 0 else ""
                    return f"background-color: {colour}" if colour else ""
                except Exception:
                    return ""

            idx_col    = "account" if "account" in df.columns else df.columns[0]
            df_display = df.set_index(idx_col)
            float_cols = [c for c in df_display.columns if df_display[c].dtype in ["float64", "float32"]]
            count_cols = [c for c in df_display.columns if any(x in c for x in ["event_count", "tp_count", "fp_count"])]
            fmt = {c: "{:.4f}" for c in float_cols}
            fmt.update({c: "{:.0f}" for c in count_cols})

            styled = df_display.style.format(fmt, na_rep="—")
            for c in fp_delta_cols:
                styled = styled.map(lambda v: colour_delta(v, lower_is_better=True),  subset=[c])
            for c in tp_delta_cols + pr_delta_cols:
                styled = styled.map(lambda v: colour_delta(v, lower_is_better=False), subset=[c])
            st.dataframe(styled, use_container_width=True)

        with acc_tab_ms:
            _render_account_tab(EV_MS,  set(MS_TP_BASE), "TP = microsleep or drowsiness")
        with acc_tab_ms_y:
            _render_account_tab(EV_MS,  set(MS_TP_YAWN), "TP = microsleep, drowsiness, or yawning")
        with acc_tab_lga:
            _render_account_tab(EV_LGA, set(LGA_TP),     "TP = long glance away, mobile device, or other distraction")

        # Daily breakdowns
        if daily_sheets:
            st.subheader("📅 Daily Breakdowns")
            for sheet_name, dfday in daily_sheets.items():
                with st.expander(sheet_name, expanded=False):
                    idx = "metric" if "metric" in dfday.columns else dfday.columns[0]
                    st.dataframe(dfday.set_index(idx), use_container_width=True)

    except Exception:
        progress.empty()
        st.error("❌ An error occurred during processing. See details below.")
        st.code(traceback.format_exc(), language="python")

    finally:
        for p in [tmp_events, tmp_trips]:
            try:
                if p and os.path.exists(p):
                    os.unlink(p)
            except Exception:
                pass
