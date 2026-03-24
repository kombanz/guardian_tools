# Guardian SW Version Performance Comparator

Repeatable performance comparison tool for Guardian software versions using Events and Trips CSV exports from Guardian Live. Available as a **Streamlit web GUI** (recommended) and a **command-line script**.

---

## Overview

This tool answers the question: *did this software version improve or regress on alert quality?*

Given two software versions and their associated event/trip data, it:

- Applies trip-level quality filters to produce reliable exposure denominators
- Identifies units with sufficient data in **both** versions (default: ≥ 30 mobile hours each)
- Computes pooled **Microsleep** and **LGA (Long Glance Away)** metrics per version:
  - Event count, TP count, FP count, Precision
  - Events / TP / FP per mobile hour
- Produces a **Δ (delta)** table for instant pass/fail interpretation
- Ranks the **Top N worst FP/hr units** per version for targeted investigation
- Optionally generates **daily breakdown** tables
- Exports all outputs to a single, structured **Excel workbook**

---

## Project Structure

```
.
├── app.py                      # Streamlit web GUI
├── run_sw_compare.py           # Core analysis logic + CLI entry point
├── requirements_sw_compare.txt # Python dependencies
└── README.md
```

---

## Setup

### 1. Create a virtual environment *(one-time)*

```bash
python -m venv .venv
```

### 2. Activate the environment

**Windows**
```bash
.venv\Scripts\activate
```

**macOS / Linux**
```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements_sw_compare.txt
```

---

## Usage — Web GUI (recommended)

```bash
python -m streamlit run app.py
```

Open **http://localhost:8501** in your browser.

| Step | Action |
|------|--------|
| **1. Upload** | Drag and drop the Events CSV and Trips CSV |
| **2. Configure** | Set version names, min-hours filter, and Top N in the sidebar |
| **3. Run** | Click **▶ Run Comparison** |
| **4. Review** | Browse metrics tables, delta comparisons, and bar charts |
| **5. Download** | Export the full Excel workbook with one click |

> The app validates your CSV columns on upload and displays a clear error if any required columns are missing.

---

## Usage — Command Line

```bash
python run_sw_compare.py \
  --events path/to/events.csv \
  --trips  path/to/trips.csv \
  --out    results.xlsx
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--events` | *(required)* | Path to the events CSV |
| `--trips` | *(required)* | Path to the trips CSV |
| `--out` | *(required)* | Output Excel workbook path (`.xlsx`) |
| `--versions` | `3.15.46,12.2.20` | Comma-separated versions to compare |
| `--min-hours` | `30` | Minimum mobile hours per unit in **both** versions |
| `--topn` | `10` | Number of worst FP/hr units to include in rankings |
| `--daily` | `none` | Daily tables: `none`, `pooled`, or `thresholded` |

---

## Required CSV Columns

**Events CSV**
| Column | Description |
|--------|-------------|
| `software_version` | Software version string |
| `guardian_unit` | Unit identifier |
| `event_type` | Alert type (e.g. `microsleep`, `lga`) |
| `classification` | Reviewer classification (e.g. `drowsiness`, `fp`) |

**Trips CSV**
| Column | Description |
|--------|-------------|
| `software_version` | Software version string |
| `guardian_unit` | Unit identifier |
| `mobile_mins` | Minutes the vehicle was in motion |
| `operating_mins` | Total operating minutes |
| `distance_kms` | Distance travelled (km) |

---

## TP / FP Definitions

| Metric variant | True Positive (TP) classifications |
|----------------|------------------------------------|
| Microsleep (base) | `microsleep`, `drowsiness` |
| Microsleep + Yawning | `microsleep`, `drowsiness`, `yawning` |
| LGA | `long glance away`, `mobile device`, `other distraction` |

All other classifications are counted as **False Positives (FP)**.

> **Note:** Rates are per *mobile* hour — alerts cannot fire while the vehicle is stationary.

---

## Trip Filtering Rules *(exposure denominators only)*

These filters affect **only** the denominator (mobile hours). Event numerators are unaffected.

| Rule | Condition |
|------|-----------|
| Short trip | `mobile_mins < 4` |
| Negative duration | `mobile_mins < 0` |
| Extreme long / low distance | `mobile_mins > 2000` AND `distance_kms < 200` |

---

## Output Excel Workbook

| Sheet | Contents |
|-------|----------|
| `README` | Run parameters, definitions, and trip filter QC |
| `Exposure_Summary` | Mobile / operating hours per version (included units) |
| `Pooled_MS_base` | Pooled Microsleep metrics — base variant |
| `Pooled_MS_yawn` | Pooled Microsleep metrics — +yawning variant |
| `Pooled_LGA` | Pooled LGA metrics |
| `Included_Units_List` | Units that met the min-hours threshold in both versions |
| `TopN_FPhr_MS_base_*` | Top N worst FP/hr units — Microsleep base, per version |
| `TopN_FPhr_MS_yawn_*` | Top N worst FP/hr units — Microsleep +yawning, per version |
| `TopN_FPhr_LGA_*` | Top N worst FP/hr units — LGA, per version |
| `Daily_*` | Daily breakdowns (only when `--daily` is set) |

---

## Dependencies

```
pandas
numpy
openpyxl
streamlit
```

Install via:
```bash
pip install -r requirements_sw_compare.txt
```

---

## Troubleshooting

| Problem | Resolution |
|---------|------------|
| *Missing column* error on upload | Ensure your CSV exports contain all required columns listed above |
| No units included in comparison | Lower `--min-hours`; units must meet the threshold in **both** versions |
| Empty pooled metrics tables | Check that `--versions` values exactly match the `software_version` values in your CSVs (case-insensitive) |
| Excel sheet name truncated | Sheet names are capped at 31 characters — this is an Excel limitation and does not affect data |
