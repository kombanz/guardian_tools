"""app.py — Streamlit GUI for the 11.4 vs 12.2 performance comparison tool.

Run with:
    streamlit run app.py
"""

import io
import os
import sys
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
# Import the core logic from run_sw_compare.py
# ---------------------------------------------------------------------------
try:
    from run_sw_compare import (
        DEFAULT_MIN_HOURS,
        DEFAULT_TOPN,
        DEFAULT_VERSIONS,
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
# Helpers
# ---------------------------------------------------------------------------

def save_upload_to_tmp(uploaded_file) -> str:
    """Save a Streamlit UploadedFile to a temp file and return its path."""
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


# ---------------------------------------------------------------------------
# Improvement #9 — Column validation
# ---------------------------------------------------------------------------

EVENTS_REQUIRED_COLS = ["software_version", "guardian_unit", "event_type", "classification"]
TRIPS_REQUIRED_COLS = ["software_version", "guardian_unit", "mobile_mins", "operating_mins", "distance_kms"]


def validate_csv_columns(df: pd.DataFrame, required: list, label: str) -> None:
    """Raise a descriptive RuntimeError if any required columns are missing."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"**{label}** is missing required column(s): {', '.join(f'`{c}`' for c in missing)}\n\n"
            f"Columns found: {', '.join(df.columns.tolist())}"
        )


# ---------------------------------------------------------------------------
# Improvement #7 — Delta comparison table
# ---------------------------------------------------------------------------

def make_comparison_df(pooled_df: pd.DataFrame) -> pd.DataFrame:
    """Reshape a 2-row pooled metrics df into metric | vA | vB | Δ(B-A) table."""
    metric_cols = [
        "mobile_hours", "event_count", "tp_count", "fp_count",
        "precision", "events_per_mobile_hour", "tp_per_mobile_hour", "fp_per_mobile_hour",
    ]
    metric_cols = [m for m in metric_cols if m in pooled_df.columns]
    if len(pooled_df) < 2:
        return pooled_df

    v1_row = pooled_df.iloc[0]
    v2_row = pooled_df.iloc[1]
    v1_name = str(v1_row.get("software_version", "Version A"))
    v2_name = str(v2_row.get("software_version", "Version B"))

    rows = []
    for m in metric_cols:
        v1_val = v1_row[m]
        v2_val = v2_row[m]
        try:
            delta = float(v2_val) - float(v1_val)
        except Exception:
            delta = None
        rows.append({"Metric": m, v1_name: v1_val, v2_name: v2_val, "Δ (B − A)": delta})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Improvement #5 — Bar chart helper
# ---------------------------------------------------------------------------

def make_bar_chart_df(pooled_df: pd.DataFrame) -> pd.DataFrame:
    """Return a df with rate metrics as rows and versions as columns for st.bar_chart."""
    rate_metrics = ["fp_per_mobile_hour", "tp_per_mobile_hour", "events_per_mobile_hour"]
    rate_metrics = [m for m in rate_metrics if m in pooled_df.columns]
    chart: dict = {}
    for _, row in pooled_df.iterrows():
        v = str(row.get("software_version", "?"))
        for m in rate_metrics:
            chart.setdefault(m, {})[v] = row[m]
    return pd.DataFrame(chart).T


# ---------------------------------------------------------------------------
# UI — Header
# ---------------------------------------------------------------------------
st.title("📊 Software Version Performance Comparator")
st.caption("Compare Guardian software versions using Events & Trips CSV exports.")

if not _import_ok:
    st.error(f"❌ Could not import core logic from **run_sw_compare.py**: `{_import_error}`\n\nMake sure `run_sw_compare.py` is in the same folder as this app.")
    st.stop()

# ---------------------------------------------------------------------------
# UI — Sidebar (settings)
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
        min_value=0.0,
        value=float(DEFAULT_MIN_HOURS),
        step=5.0,
        help="Units must have AT LEAST this many mobile hours in each version to be included in the comparison.",
    )
    top_n = st.number_input(
        "Top N worst FP/hr units",
        min_value=1,
        max_value=100,
        value=DEFAULT_TOPN,
        step=1,
        help="How many of the worst-performing units to show in the rankings.",
    )

    st.divider()
    st.subheader("Daily breakdown")
    daily_mode = st.selectbox(
        "Daily tables",
        options=["none", "pooled", "thresholded"],
        index=0,
        help=(
            "**none** – no daily sheets  \n"
            "**pooled** – daily metrics for ALL units  \n"
            "**thresholded** – daily metrics for included units only"
        ),
    )

    st.divider()
    st.caption(
        f"Trip filter rules applied to exposure:  \n"
        f"• Exclude trips < {TRIP_RULE_MIN_MOBILE_MINS} mobile mins  \n"
        f"• Exclude negative mobile mins  \n"
        f"• Exclude trips > {TRIP_RULE_EXTREME_MOBILE_MINS} mins AND distance < {TRIP_RULE_EXTREME_MAX_DISTANCE_KMS} km"
    )

# ---------------------------------------------------------------------------
# UI — File uploads
# ---------------------------------------------------------------------------
st.subheader("1️⃣  Upload your CSV files")

col_ev, col_tr = st.columns(2)
with col_ev:
    events_file = st.file_uploader(
        "Events CSV",
        type=["csv"],
        help="The Guardian Live events export CSV.",
    )
with col_tr:
    trips_file = st.file_uploader(
        "Trips CSV",
        type=["csv"],
        help="The Guardian Live trips export CSV.",
    )

# Output filename
st.subheader("2️⃣  Name your output file")
out_filename = st.text_input(
    "Output Excel filename",
    value="results_comparison.xlsx",
    help="The name of the Excel workbook that will be generated.",
)
if not out_filename.endswith(".xlsx"):
    out_filename += ".xlsx"

# ---------------------------------------------------------------------------
# UI — Run button
# ---------------------------------------------------------------------------
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
    log = st.empty()

    def log_msg(msg: str):
        log.info(msg)

    tmp_events = tmp_trips = None
    try:
        # --- Save uploads to temp files ---
        progress.progress(5, text="Saving uploaded files…")
        tmp_events = save_upload_to_tmp(events_file)
        tmp_trips = save_upload_to_tmp(trips_file)

        # --- Load ---
        progress.progress(10, text="Loading Events CSV…")
        events_all = load_events(tmp_events)

        progress.progress(20, text="Loading Trips CSV…")
        trips_all = load_trips(tmp_trips)

        # --- Validate columns (#9) ---
        progress.progress(22, text="Validating CSV columns…")
        try:
            validate_csv_columns(events_all, EVENTS_REQUIRED_COLS, "Events CSV")
            validate_csv_columns(trips_all, TRIPS_REQUIRED_COLS, "Trips CSV")
        except RuntimeError as _ve:
            progress.empty()
            log.empty()
            st.error(f"❌ Column validation failed:\n\n{_ve}")
            st.stop()

        # --- Name maps ---
        progress.progress(25, text="Building account/fleet name maps…")
        combined_names = pd.concat(
            [
                events_all[[c for c in ["account_id", "account", "fleet_id", "fleet"] if c in events_all.columns]].copy(),
                trips_all[[c for c in ["account_id", "account", "fleet_id", "fleet"] if c in trips_all.columns]].copy(),
            ],
            axis=0,
            ignore_index=True,
        )
        id_maps = {}
        if "account_id" in combined_names.columns and "account" in combined_names.columns:
            id_maps["account_id"] = mode_clean_label(combined_names, "account_id", "account")
        if "fleet_id" in combined_names.columns and "fleet" in combined_names.columns:
            id_maps["fleet_id"] = mode_clean_label(combined_names, "fleet_id", "fleet")

        # --- Trip filtering ---
        progress.progress(35, text="Applying trip exposure filters…")
        trips_kept, qc, _ = apply_trip_filters(trips_all)

        # --- Exposure ---
        progress.progress(45, text="Computing exposure (mobile hours)…")
        exposure = compute_exposure(trips_kept)

        # --- Included units ---
        progress.progress(55, text="Determining included units…")
        included_units = determine_included_units(exposure, versions=versions, min_hours=min_hours)
        exposure_totals = exposure_totals_for_included(exposure, included_units, versions=versions)

        # --- Events for included units ---
        progress.progress(60, text="Filtering events to included units…")
        events_inc = _prep_events_for_metrics(events_all, versions=versions, included_units=included_units)

        # --- Pooled metrics ---
        progress.progress(65, text="Computing pooled metrics…")
        pooled_ms_base, pooled_ms_yawn, pooled_lga = compute_pooled_metrics(events_inc, exposure_totals, versions=versions)

        # --- Unit rankings ---
        progress.progress(72, text="Computing unit-level FP/hr rankings…")
        top_ms_base = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="ms_base", topn=top_n, id_maps=id_maps)
        top_ms_yawn = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="ms_yawn", topn=top_n, id_maps=id_maps)
        top_lga = compute_unit_level_rates(events_inc, exposure, versions=versions, variant="lga", topn=top_n, id_maps=id_maps)

        # --- Daily tables ---
        progress.progress(80, text="Building daily breakdowns…")
        daily_sheets = compute_daily_tables(
            events_all=events_all,
            trips_kept=trips_kept,
            included_units=included_units,
            versions=versions,
            mode=daily_mode,
        )

        # --- Export to in-memory Excel ---
        progress.progress(88, text="Writing Excel workbook…")
        readme_df = build_readme_sheet(
            events_path=events_file.name,
            trips_path=trips_file.name,
            versions=versions,
            min_hours=min_hours,
            qc=qc,
            daily_mode=daily_mode,
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
        log.empty()

        # -----------------------------------------------------------------------
        # Results display
        # -----------------------------------------------------------------------
        st.success("✅ Comparison complete! Download your workbook below, or browse the results here.")

        # Download button
        st.download_button(
            label="⬇️  Download Excel Workbook",
            data=excel_buffer,
            file_name=out_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.divider()

        # --- QC summary ---
        with st.expander("🔍 Trip Filter QC", expanded=False):
            qc_data = {
                "Metric": [
                    "Total trip rows loaded",
                    f"Excluded — short mobile (< {TRIP_RULE_MIN_MOBILE_MINS} mins)",
                    "Excluded — negative mobile mins",
                    f"Excluded — extreme long + low distance",
                    "✅ Kept for exposure",
                ],
                "Count": [
                    fmt_num(qc.total_rows),
                    fmt_num(qc.excluded_short_mobile),
                    fmt_num(qc.excluded_negative_mobile),
                    fmt_num(qc.excluded_extreme_long_lowdist),
                    fmt_num(qc.kept_rows),
                ],
            }
            st.table(pd.DataFrame(qc_data))

        # --- Exposure summary ---
        st.subheader("📦 Exposure Summary")
        st.caption(f"**{len(included_units):,}** units met the ≥ {min_hours:g} mobile-hour threshold in BOTH versions.")
        st.dataframe(exposure_totals, use_container_width=True)

        # --- Pooled metrics tabs ---
        st.subheader("📈 Pooled Metrics")
        tab_ms, tab_ms_y, tab_lga = st.tabs([
            "Microsleep (base)",
            "Microsleep + Yawning",
            "LGA",
        ])

        def _render_pooled_tab(pooled_df: pd.DataFrame, caption: str) -> None:
            """Render comparison table with Δ column and bar chart."""
            st.caption(caption)

            # Delta comparison table (#7)
            cmp = make_comparison_df(pooled_df)
            float_cols = [c for c in cmp.columns if c not in ["Metric"]]
            fmt = {c: "{:.4f}" for c in float_cols}
            st.dataframe(
                cmp.style.format(fmt, na_rep="—"),
                use_container_width=True,
            )

            # Bar chart — rates per mobile hour (#5)
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

        # --- Top N rankings ---
        st.subheader(f"🏆 Top {top_n} Units by FP/hr")
        for variant_label, top_dict in [
            ("Microsleep (base)", top_ms_base),
            ("Microsleep + Yawning", top_ms_yawn),
            ("LGA", top_lga),
        ]:
            with st.expander(f"{variant_label}", expanded=False):
                for v, dfv in top_dict.items():
                    st.markdown(f"**Version: `{v}`**")
                    rate_cols = [c for c in dfv.columns if "per_mobile_hour" in c]
                    st.dataframe(
                        dfv.style.format({c: "{:.4f}" for c in rate_cols if c in dfv.columns}, na_rep="—"),
                        use_container_width=True,
                    )

        # --- Daily sheets preview ---
        if daily_sheets:
            st.subheader("📅 Daily Breakdowns")
            for sheet_name, dfday in daily_sheets.items():
                with st.expander(sheet_name, expanded=False):
                    st.dataframe(dfday, use_container_width=True)

    except Exception:
        progress.empty()
        log.empty()
        st.error("❌ An error occurred during processing. See details below.")
        st.code(traceback.format_exc(), language="python")

    finally:
        # Clean up temp files
        for p in [tmp_events, tmp_trips]:
            try:
                if p and os.path.exists(p):
                    os.unlink(p)
            except Exception:
                pass
