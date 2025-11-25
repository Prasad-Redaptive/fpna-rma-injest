#!/usr/bin/env python3
"""
Integrated Frontend + Backend: Rating & Margins Report Automation
----------------------------------------------------------------
Combines:
- Streamlit frontend for file upload
- Backend processing with 3-stage transformation
- Terminal-style log display in the UI (custom styled, scrollable)
- Local file storage with versioning
- Databricks OAuth (client_credentials) token fetch + auto-refresh (stores in browser localStorage)
"""

import os
import re
import csv
import hashlib
import argparse
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
import datetime as dt
import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_object_dtype, is_numeric_dtype
import streamlit as st
from io import StringIO
import threading
import queue
import html  # <-- for safe HTML escaping in the custom log window
import time
import requests
import streamlit.components.v1 as components

try:
    from dateutil import parser as _dateparse
    _HAVE_DATEUTIL = True
except Exception:
    _HAVE_DATEUTIL = False

# OPTIONAL: smooth auto-rerun helper
try:
    from streamlit_autorefresh import st_autorefresh
    _HAVE_AUTOREFRESH = True
except Exception:
    _HAVE_AUTOREFRESH = False


# =============================
# Configuration
# =============================

# Frontend Configuration
CATALOG   = os.getenv("APP_CATALOG", "main")
SCHEMA    = os.getenv("APP_SCHEMA", "rating_margins")
VOLUME    = os.getenv("APP_VOLUME", "uploads_volume")
LOG_TABLE = os.getenv("APP_LOG_TABLE", "upload_log")
RAW_DIR   = os.getenv("APP_RAW_DIR", "raw_uploads")
MAX_MB    = int(os.getenv("APP_MAX_MB", "50"))
LOCAL_ROOT = Path(os.getenv("APP_LOCAL_STORAGE_ROOT", Path.cwd() / "local_uc"))

# Backend Configuration
DEFAULT_SHEETS = [
    "Rating Analysis",
    "Project Economics",
    "Capex & Audit deals",
]

CAPEX_SHEET_ALIASES = [
    "Capex & Audit deals",
    "Capex and Audit deals",
    "Capex & audit deals",
    "Capex and audit deals",
]

PROJECT_ECON_SHEET_ALIASES = [
    "Project Economics",
    "project economics",
]

RATING_ANALYSIS_SHEET_ALIASES = [
    "Rating Analysis",
    "rating analysis",
]

# Capex forced-date columns
CAPEX_DATE_COLUMNS = [
    "actual_ntp_date",
    "loa_date",
    "termination_date",
    "commercial_operation_date",
    "loa_signed_f",
    "ntp_contract_created_date",
    "superseding_ntp_date",
]

# Project Economics forced-date columns
PROJECT_ECON_DATE_COLUMNS = [
    "Actual NTP Date",
    "LOA Date",
    "Commercial Operation Date",
    "LOA Signed (F)",
    "NTP Contract Created Date",
    "NTP Contract: Created Date",
    "Superseding NTP Date",
    "Actual/Projected LOA date",
    "LOA Month end date",
    "COD Month End Date",
    "NTP Month End Date",
    "NTP Contract End Date",
]

CAPEX_NUMERIC_COLUMN = "net_revenue_converted"

# Regex patterns
DATEISH_RE = re.compile(
    r"""^\s*
        \d{1,4}[/\-\.]\d{1,2}[/\-\.]\d{1,4}
        (?:[ T]\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?
        \s*(?:AM|PM|am|pm)?)?
        \s*$""",
    re.X | re.IGNORECASE,
)

UNNAMED_CELL_RE = re.compile(r'^\s*Unnamed[:\s].*$', re.IGNORECASE)
UNNAMED_COLUMN_RE = re.compile(r'^\s*Unnamed:\s*\d+\s*$', re.IGNORECASE)

# ===== Databricks OAuth (Client Credentials) Config =====
DBX_TOKEN_URL = os.getenv(
    "DBX_TOKEN_URL",
    "https://dbc-c686a1d6-b88c.cloud.databricks.com/oidc/v1/token"
)

# NOTE: Keep this out of code in prod. Set env var DBX_BASIC_B64 = base64("<client_id>:<client_secret>")
DBX_BASIC_B64 = os.getenv(
    "DBX_BASIC_B64",
    # fallback ONLY for local testing; replace in production
    "NjhjNmY1MmQtZmM0Ny00OTM5LThiZjctZDU5NWE4OGQ1ZGQ1OmRvc2ViYjMxMGJmYjhhOThiN2FkYzcxN2M1NjQ1ZGQ1MjE4ZA=="
)

# LocalStorage keys (browser side)
LS_TOKEN_KEY = "dbx_access_token"
LS_EXP_KEY   = "dbx_access_exp_epoch"

# Small skew so we refresh a bit before real expiry
EXP_SKEW_SECONDS = 60

def _epoch_now() -> int:
    return int(time.time())

# =============================
# Global Logging Setup
# =============================

class StreamlitOutput:
    """Capture stdout/stderr for Streamlit display (custom styled, scrollable)."""
    def __init__(self, placeholder, max_lines=1000):
        self.placeholder = placeholder
        self.max_lines = max_lines
        self.lines = []
        self._lock = threading.Lock()
    
    def write(self, text):
        if text.strip():
            with self._lock:
                self.lines.append(f"{text}")
                # Keep only recent lines
                if len(self.lines) > self.max_lines:
                    self.lines = self.lines[-self.max_lines:]
                # Update the Streamlit display
                self.update_display()
    
    def update_display(self):
        # Use empty string if no content yet
        content = "\n".join(self.lines) if self.lines else "Processing logs will appear here..."
        # Render inside a fixed-height, scrollable dark themed box with monospace font
        self.placeholder.markdown(
            f"""
<div class="log-window"><pre>{html.escape(content)}</pre></div>
""",
            unsafe_allow_html=True,
        )
    
    def flush(self):
        pass

# =============================
# Frontend Utilities
# =============================

def setup_frontend_directories():
    """Setup local storage directories"""
    UPLOAD_DIR = LOCAL_ROOT / CATALOG / SCHEMA / VOLUME / RAW_DIR
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    LOG_CSV = LOCAL_ROOT / CATALOG / SCHEMA / f"{LOG_TABLE}.csv"
    LOG_CSV.parent.mkdir(parents=True, exist_ok=True)
    
    return UPLOAD_DIR, LOG_CSV

def hash_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(name or "file"))

def log_event_csv(log_csv_path, row: dict):
    LOG_COLUMNS = ["event_time", "filename", "stored_path", "filesize_bytes", "file_hash", "status", "message"]
    file_exists = log_csv_path.exists()
    with log_csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(LOG_COLUMNS)
        w.writerow([row.get(c, "") for c in LOG_COLUMNS])

# =============================
# Databricks Token Helpers
# =============================

def fetch_dbx_token_client_credentials(scope: str = "all-apis") -> tuple[str, int] | tuple[None, None]:
    """
    Databricks OIDC /token (client_credentials) -> (access_token, exp_epoch).
    Returns (None, None) on failure. Keeps client secret server-side.
    """
    try:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {DBX_BASIC_B64}"
        }
        data = {
            "grant_type": "client_credentials",
            "scope": scope
        }
        resp = requests.post(DBX_TOKEN_URL, headers=headers, data=data, timeout=(5, 20))
        if not resp.ok:
            print(f"[DBX TOKEN] HTTP {resp.status_code}: {resp.text[:300]}")
            return None, None
        j = resp.json()
        tok = j.get("access_token")
        expires_in = int(j.get("expires_in", 0))
        if not tok or expires_in <= 0:
            print("[DBX TOKEN] Missing access_token/expires_in in response")
            return None, None
        exp_epoch = _epoch_now() + max(0, expires_in - EXP_SKEW_SECONDS)
        return tok, exp_epoch
    except Exception as e:
        print(f"[DBX TOKEN] Error: {e}")
        return None, None

def write_token_to_local_storage_js(token: str, exp_epoch: int):
    """
    Writes token & expiry into the browser's localStorage.
    (We never read secrets from browser; this is for convenience in frontends that need the token.)
    """
    safe_token = token.replace("\\", "\\\\").replace("`", "\\`")
    html_js = f"""
    <script>
      try {{
        localStorage.setItem("{LS_TOKEN_KEY}", `{safe_token}`);
        localStorage.setItem("{LS_EXP_KEY}", String({exp_epoch}));
        console.log("DBX token stored. Expires @", new Date({exp_epoch}*1000).toISOString());
      }} catch (e) {{
        console.error("localStorage write failed:", e);
      }}
    </script>
    """
    components.html(html_js, height=0, scrolling=False)

def ensure_token_in_session_and_localstorage(scope: str = "all-apis"):
    """
    - Check st.session_state for token & expiry
    - If missing/expired -> fetch new
    - Mirror to browser localStorage via JS
    - Return (token, exp_epoch) or (None, None)
    """
    if "dbx_token" not in st.session_state:
        st.session_state.dbx_token = None
    if "dbx_token_exp" not in st.session_state:
        st.session_state.dbx_token_exp = 0

    token = st.session_state.dbx_token
    exp   = int(st.session_state.dbx_token_exp or 0)
    now = _epoch_now()

    needs_refresh = (not token) or (exp <= now)
    if needs_refresh:
        new_tok, new_exp = fetch_dbx_token_client_credentials(scope=scope)
        if not new_tok:
            return None, None
        st.session_state.dbx_token = new_tok
        st.session_state.dbx_token_exp = new_exp
        write_token_to_local_storage_js(new_tok, new_exp)
        return new_tok, new_exp

    # Still valid -> mirror to localStorage (new tab/cleared storage cases)
    write_token_to_local_storage_js(token, exp)
    return token, exp

# =============================
# Backend Helper Functions
# =============================

def sanitize_filename(name: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*]+', "_", str(name))
    return safe.strip().strip(".") or "sheet"

def _canon_sheet_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    return s

def _is_dtlike_scalar(x) -> bool:
    return isinstance(x, (pd.Timestamp, dt.datetime, dt.date, np.datetime64))

def _format_dt_series_to_date_strings(dt_series: pd.Series, fmt: str) -> pd.Series:
    out = pd.Series(None, index=dt_series.index, dtype="object")
    mask = dt_series.notna()
    out.loc[mask] = dt_series.loc[mask].dt.strftime(fmt)
    return out

def _best_dayfirst(series_str: pd.Series, prefer_dayfirst: Optional[bool]) -> bool:
    if prefer_dayfirst is not None:
        return prefer_dayfirst
    s = series_str.dropna().astype(str)
    if s.empty:
        return False
    sample = s.iloc[:200]
    tokens = sample.str.extract(r'^\s*(\d{1,4})[/\-](\d{1,2})[/\-](\d{1,4})', expand=True)
    if not tokens.empty and (pd.to_numeric(tokens[1], errors="coerce") > 12).any():
        return True
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        dt_df = pd.to_datetime(sample, errors="coerce", dayfirst=True)
        dt_mf = pd.to_datetime(sample, errors="coerce", dayfirst=False)
    return dt_df.notna().sum() >= dt_mf.notna().sum()

def _detect_excel_origin(xlsm_path: Path) -> str:
    """Return origin for Excel serials: '1899-12-30' (1900) or '1904-01-01' (1904)."""
    try:
        from openpyxl import load_workbook
        wb = load_workbook(filename=xlsm_path, read_only=True, data_only=True, keep_vba=True)
        use_1904 = bool(getattr(wb.properties, "date1904", False))
        return "1904-01-01" if use_1904 else "1899-12-30"
    except Exception as e:
        print(f"Warning: Could not read workbook date base, assuming 1900 system: {e}")
        return "1899-12-30"

def is_date_header(header: str) -> bool:
    """Whole-word 'date' in header, case-insensitive."""
    h = "" if header is None else str(header)
    return re.search(r'\bdate\b', h, flags=re.IGNORECASE) is not None

def map_existing_columns(df: pd.DataFrame, wanted: List[str]) -> Dict[str, str]:
    """Map {wanted_name -> actual_df_column} with normalization."""
    def norm(x: str) -> str:
        s = "" if x is None else str(x)
        s = s.lower()
        s = re.sub(r'[^0-9a-z]+', ' ', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s
    existing_map = {norm(c): c for c in df.columns}
    return {w: existing_map[norm(w)] for w in wanted if norm(w) in existing_map}

def _serial_from_python_dt(d: dt.datetime, origin_dt: dt.datetime) -> float:
    delta = d - origin_dt
    return delta.days + (delta.seconds + delta.microseconds / 1e6) / 86400.0

def _coerce_to_py_datetime(val, *, dayfirst: Optional[bool]) -> Optional[dt.datetime]:
    """Best-effort conversion of a scalar to Python datetime, overflow-safe."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, dt.datetime):
        return val
    if isinstance(val, dt.date):
        return dt.datetime(val.year, val.month, val.day)
    if isinstance(val, pd.Timestamp):
        try:
            return val.to_pydatetime()
        except Exception:
            return None
    if isinstance(val, np.datetime64):
        try:
            return pd.Timestamp(val).to_pydatetime()
        except Exception:
            return None
    s = str(val).strip()
    if not DATEISH_RE.match(s):
        return None
    if _HAVE_DATEUTIL:
        try:
            return _dateparse.parse(s, dayfirst=bool(dayfirst))
        except Exception:
            try:
                return _dateparse.parse(s, dayfirst=not bool(dayfirst))
            except Exception:
                return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            ts = pd.to_datetime(s, errors="coerce", dayfirst=bool(dayfirst))
        except Exception:
            return None
    if pd.isna(ts):
        return None
    try:
        return ts.to_pydatetime()
    except Exception:
        return None

def _to_excel_serial_from_dt_safe(series_like: pd.Series, origin: str, *, dayfirst: Optional[bool]) -> pd.Series:
    """Element-wise safe conversion of datetime-like values to Excel serial numbers."""
    origin_dt = dt.datetime.strptime(origin, "%Y-%m-%d")
    def conv(x):
        d = _coerce_to_py_datetime(x, dayfirst=dayfirst)
        if d is None:
            return np.nan
        try:
            return _serial_from_python_dt(d, origin_dt)
        except Exception:
            return np.nan
    return series_like.apply(conv)

def scrub_unnamed_cells(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    obj_cols = out.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        s = out[col].astype("object")
        mask = s.astype(str).str.match(UNNAMED_CELL_RE, na=False)
        if mask.any():
            s.loc[mask] = ""
            out[col] = s
    return out

def remove_unnamed_columns(df: pd.DataFrame, verbose: bool = False, sheet_name: str = "") -> pd.DataFrame:
    unnamed_cols = [col for col in df.columns if UNNAMED_COLUMN_RE.match(str(col))]
    if unnamed_cols:
        if verbose:
            print(f"[{sheet_name}] Removing {len(unnamed_cols)} unnamed columns: {', '.join(map(str, unnamed_cols))}")
        df = df.drop(columns=unnamed_cols)
    else:
        if verbose:
            print(f"[{sheet_name}] No unnamed columns to remove.")
    return df

def _clean_numeric_text_soft(s: pd.Series) -> pd.Series:
    if not is_object_dtype(s):
        return s
    txt = s.astype(str).str.strip()
    txt = txt.str.replace(r'^\(\s*([^)]+?)\s*\)$', r'-\1', regex=True)
    txt = txt.str.replace(r'[\u00A0\u202F]', '', regex=True)
    txt = txt.str.replace(r'[$€£₹]', '', regex=True)
    txt = txt.str.replace(',', '', regex=False)
    return txt

def _to_numeric_preserve_original(
    s: pd.Series, *, excel_origin: str, verbose: bool = False, label: str = ""
) -> pd.Series:
    """Try numeric; if fail, convert date-like to Excel serials safely; keep original where still unparseable."""
    if is_numeric_dtype(s):
        if verbose: print(f"[numeric untouched] {label or s.name}")
        return s

    if is_datetime64_any_dtype(s):
        serial = _to_excel_serial_from_dt_safe(s.astype("object"), excel_origin, dayfirst=None)
        out = s.astype("object")
        ok = pd.notna(serial)
        out.loc[ok] = serial.loc[ok]
        if verbose: print(f"[datetime→serial({excel_origin}) safe] {label or s.name}: {ok.sum()} cells")
        return out

    raw = s.astype("object")
    out = raw.copy()

    cleaned = _clean_numeric_text_soft(raw)
    parsed_num = pd.to_numeric(cleaned, errors="coerce")
    num_ok = parsed_num.notna()
    if num_ok.any():
        out.loc[num_ok.index[num_ok]] = parsed_num.loc[num_ok]

    remainder_idx = out.index[~num_ok]
    if len(remainder_idx) > 0:
        rem = raw.loc[remainder_idx]
        rem_str = rem.astype(str)
        mask_dateish = rem_str.str.match(DATEISH_RE, na=False)
        if mask_dateish.any():
            subset = rem_str[mask_dateish]
            dayfirst_guess = _best_dayfirst(subset, prefer_dayfirst=None)
            serial = _to_excel_serial_from_dt_safe(subset, excel_origin, dayfirst=dayfirst_guess)
            ok = pd.notna(serial)
            if ok.any():
                out.loc[serial.index[ok]] = serial.loc[ok]
                if verbose:
                    print(f"[str-date→serial({excel_origin}) safe] {label or s.name}: {ok.sum()} cells")

    if verbose:
        changed = (out != raw).sum()
        print(f"[numeric/date parsed, preserved originals] {label or s.name}: {int(changed)} converted, {int((out == raw).sum())} kept")
    return out

def _coerce_series_to_date_forced_allow_serials(
    s: pd.Series,
    *,
    date_fmt: str,
    prefer_dayfirst: Optional[bool],
    excel_origin: str,
    verbose: bool = False,
    label: str = "",
) -> pd.Series:
    if is_datetime64_any_dtype(s):
        dt_series = pd.to_datetime(s, errors="coerce")
        out = _format_dt_series_to_date_strings(dt_series, date_fmt).where(dt_series.notna(), s.astype("object"))
        if verbose: print(f"[datetime→{date_fmt}] {label or s.name}")
        return out

    if is_numeric_dtype(s):
        num = pd.to_numeric(s, errors="coerce")
        dt_series = pd.to_datetime(num, unit="D", origin=excel_origin, errors="coerce")
        out = _format_dt_series_to_date_strings(dt_series, date_fmt).where(dt_series.notna(), s.astype("object"))
        if verbose: print(f"[serial({excel_origin})→{date_fmt}] {label or s.name}")
        return out

    if is_object_dtype(s):
        s_obj = s.astype("object").copy()

        mask_dt = s_obj.apply(_is_dtlike_scalar)
        if mask_dt.any():
            dt_series = pd.to_datetime(s_obj[mask_dt], errors="coerce")
            ok = dt_series.notna()
            if ok.any():
                s_obj.loc[mask_dt[mask_dt].index[ok]] = dt_series.loc[ok].dt.strftime(date_fmt)
                if verbose: print(f"[object dt-cells→{date_fmt}] {label or s.name}: {ok.sum()} cells")

        num_try = pd.to_numeric(_clean_numeric_text_soft(s_obj), errors="coerce")
        mask_num = num_try.notna()
        if mask_num.any():
            dt_series = pd.to_datetime(num_try[mask_num], unit="D", origin=excel_origin, errors="coerce")
            okn = dt_series.notna()
            if okn.any():
                idx = num_try[mask_num].index[okn]
                s_obj.loc[idx] = dt_series.loc[okn].dt.strftime(date_fmt)
                if verbose: print(f"[str-serial({excel_origin})→{date_fmt}] {label or s.name}: {okn.sum()} cells")

        s_str = s_obj.astype(str)
        mask_str_dateish = s_str.str.match(DATEISH_RE, na=False)
        if mask_str_dateish.any():
            dayfirst = _best_dayfirst(s_str[mask_str_dateish], prefer_dayfirst)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                parsed = pd.to_datetime(s_str[mask_str_dateish], errors="coerce", dayfirst=dayfirst)
            ok2 = parsed.notna()
            if ok2.any():
                idx = s_str[mask_str_dateish].index[ok2]
                s_obj.loc[idx] = parsed.loc[ok2].dt.strftime(date_fmt)
                if verbose: print(f"[parse date strings→{date_fmt}] {label or s.name}: {ok2.sum()} cells")

        return s_obj

    return s

def _coerce_series_to_date_strict_no_numeric(
    s: pd.Series,
    *,
    date_fmt: str,
    prefer_dayfirst: Optional[bool],
    verbose: bool = False,
    label: str = "",
) -> pd.Series:
    if is_datetime64_any_dtype(s):
        dt_series = pd.to_datetime(s, errors="coerce")
        out = _format_dt_series_to_date_strings(dt_series, date_fmt).where(dt_series.notna(), s.astype("object"))
        if verbose: print(f"[dtype=datetime→{date_fmt}] {label or s.name}")
        return out

    if is_object_dtype(s):
        s_obj = s.astype("object").copy()
        mask_dt = s_obj.apply(_is_dtlike_scalar)
        if mask_dt.any():
            dt_series = pd.to_datetime(s_obj[mask_dt], errors="coerce")
            ok = dt_series.notna()
            if ok.any():
                s_obj.loc[mask_dt[mask_dt].index[ok]] = dt_series.loc[ok].dt.strftime(date_fmt)
                if verbose: print(f"[object dt-cells→{date_fmt}] {label or s.name}: {ok.sum()} cells")

        s_str = s_obj.astype(str)
        mask_str_dateish = s_str.str.match(DATEISH_RE, na=False)
        if mask_str_dateish.any():
            dayfirst = _best_dayfirst(s_str[mask_str_dateish], prefer_dayfirst)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                parsed = pd.to_datetime(s_str[mask_str_dateish], errors="coerce", dayfirst=dayfirst)
            ok2 = parsed.notna()
            if ok2.any():
                idx = s_str[mask_str_dateish].index[ok2]
                s_obj.loc[idx] = parsed.loc[ok2].dt.strftime(date_fmt)
                if verbose: print(f"[strings→{date_fmt}] {label or s.name}: {ok2.sum()} cells")
        return s_obj

    if is_numeric_dtype(s):
        if verbose: print(f"[numeric untouched] {label or s.name}")
    return s

def safe_to_csv(df: pd.DataFrame, out_path: Path, **kwargs) -> Path:
    try:
        df.to_csv(out_path, **kwargs)
        return out_path
    except PermissionError:
        for i in range(1, 51):
            candidate = out_path.with_name(f"{out_path.stem} ({i}){out_path.suffix}")
            try:
                df.to_csv(candidate, **kwargs)
                return candidate
            except PermissionError:
                continue
        raise

def standardize_capex_columns(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Standardize Capex column names to match the required structure."""
    column_mapping = {
        'Opportunity Record ID': 'opportunity_record_id',
        'Parent Account of Proj Oppty': 'parent_account_of_proj_oppty',
        'Opportunity Name': 'opportunity_name', 
        'Customer Parent Opportunity': 'customer_parent_opportunity',
        'Stage': 'stage',
        'Actual NTP Date': 'actual_ntp_date',
        'LOA Date': 'loa_date',
        'Termination Date': 'termination_date',
        'Opportunity : NTP Contract : Contract Term (Months)': 'ntp_contract_term_months',
        'Opportunity : LOA Contract : Contract Term (Months)': 'loa_contract_term_months',
        'NTP Contract Value (converted)': 'ntp_contract_value_converted',
        'LOA Contract Value (converted)': 'loa_contract_value_converted',
        'Net Revenue (converted)': 'net_revenue_converted',
        'Total Net Cost (converted)': 'total_net_cost_converted',
        'Commercial Operation Date': 'commercial_operation_date',
        'GTM Team': 'gtm_team',
        'Opportunity Type': 'opportunity_type',
        'Opportunity Type Lvl 2': 'opportunity_type_lvl2',
        'Exclude from Financial Reporting': 'exclude_from_financial_reporting',
        'Deal Type': 'deal_type',
        'Opportunity Owner': 'opportunity_owner',
        'NTP Contract: NPV Margin %': 'ntp_contract_npv_margin_pct',
        'NTP Contract: O&M Cost (converted)': 'ntp_contract_o_and_m_cost_converted',
        'Billing Structure': 'billing_structure',
        'LOA Signed (F)': 'loa_signed_f',
        'NTP Contract: Created Date': 'ntp_contract_created_date',
        'NTP Contract: NPV Project Cash Flow (value) (converted)': 'ntp_contract_npv_project_cash_flow_converted',
        'Opportunity : Parent Account of Proj Oppty : Industry Lvl 1': 'industry_lvl1',
        'Opportunity : Parent Account of Proj Oppty : Formal Credit Rating': 'formal_credit_rating',
        'NTP Contract: PV Revenue (PV of all Cash Inflow) (converted)': 'ntp_contract_pv_revenue_of_all_cash_inflow_converted',
        'NTP Contract: IRR %': 'ntp_contract_irr_pct',
        'Funding Type': 'funding_type',
        'Opportunity : Parent Account of Proj Oppty : Account Tier': 'account_tier',
        'Capital Business Type': 'capital_business_type',
        'Opportunity : Customer Parent Opportunity : Financing Type': 'financing_type',
        'Interest Rate Spread': 'interest_rate_spread',
        'Superseding NTP Date': 'superseding_ntp_date',
        'NTP Contract: Partner Fee (converted)': 'ntp_contract_partner_fee_converted',
        'Booked Customer Contract Value (converted)': 'booked_customer_contract_value_converted',
        'Opportunity : NTP Contract : Upfront Referral Cost (converted)': 'ntp_contract_upfront_referral_cost_converted'
    }
    
    actual_mapping = {}
    for current_col in df.columns:
        current_str = str(current_col)
        if current_str in column_mapping:
            actual_mapping[current_col] = column_mapping[current_str]
        else:
            found = False
            for orig_name, new_name in column_mapping.items():
                if current_str.lower() == orig_name.lower():
                    actual_mapping[current_col] = new_name
                    found = True
                    break
            if not found:
                snake_case = current_str.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(':', '').replace('%', 'pct').replace('&', 'and')
                snake_case = re.sub(r'[^a-z0-9_]', '', snake_case)
                snake_case = re.sub(r'_+', '_', snake_case).strip('_')
                actual_mapping[current_col] = snake_case
    
    if actual_mapping:
        df = df.rename(columns=actual_mapping)
        if verbose:
            renamed_cols = [f"{old} -> {new}" for old, new in actual_mapping.items() if str(old) != new]
            if renamed_cols:
                print(f"[Capex] Renamed {len(renamed_cols)} columns to standardized names:")
                for change in renamed_cols:
                    print(f"  {change}")
    
    return df

def add_capex_metadata_columns(df: pd.DataFrame, xlsm_path: Path) -> pd.DataFrame:
    """Add ingestion_date and file_name columns to Capex dataframe."""
    current_time = dt.datetime.now()
    ingestion_date_formatted = current_time.strftime("%m/%d/%Y  %I:%M:%S %p")
    df['ingestion_date'] = ingestion_date_formatted
    df['file_name'] = xlsm_path.name
    df['file_version'] = 1
    return df


# =============================
# Script 1 - Export Workbook Function
# =============================

def export_workbook(
    xlsm_path: Path,
    out_dir: Path,
    sheets_wanted: Optional[List[str]],
    *,
    date_fmt: str,
    prefer_dayfirst: Optional[bool],
    verbose: bool,
    base_filename: str
):
    """Export workbook sheets to CSV files (Script 1 functionality)"""
    try:
        xls = pd.ExcelFile(xlsm_path, engine="openpyxl")
    except Exception as e:
        raise SystemExit(f"Failed to open workbook '{xlsm_path}': {e}")

    excel_origin = _detect_excel_origin(xlsm_path)
    desired = sheets_wanted if sheets_wanted else DEFAULT_SHEETS

    exported = 0
    csv_files = {}
    
    for requested in desired:
        canon_req = _canon_sheet_name(requested)
        actual = next((s for s in xls.sheet_names if _canon_sheet_name(s) == canon_req), None)
        if actual is None:
            print(f"Warning: sheet '{requested}' not found. Available: {', '.join(xls.sheet_names)}")
            continue

        canon = _canon_sheet_name(actual)
        is_capex  = canon in [_canon_sheet_name(s) for s in CAPEX_SHEET_ALIASES]
        is_proj   = canon in [_canon_sheet_name(s) for s in PROJECT_ECON_SHEET_ALIASES]
        is_rating = canon in [_canon_sheet_name(s) for s in RATING_ANALYSIS_SHEET_ALIASES]

        # Read sheet
        try:
            if is_proj:
                df = pd.read_excel(xls, sheet_name=actual, engine="openpyxl", header=2)
                if verbose:
                    print(f"[Project Economics] Using 3rd row as header (header=2).")
            elif is_rating:
                df = pd.read_excel(xls, sheet_name=actual, engine="openpyxl", header=1)
                if verbose:
                    print(f"[Rating Analysis] Using 2nd row as header (header=1).")
            else:
                df = pd.read_excel(xls, sheet_name=actual, engine="openpyxl")
        except Exception as e:
            print(f"Error reading sheet '{actual}': {e}")
            continue
        
        # For Capex sheet: standardize columns and add metadata
        if is_capex:
            df = standardize_capex_columns(df, verbose=verbose)
            df = add_capex_metadata_columns(df, xlsm_path)
            if verbose:
                print(f"[Capex] Added metadata columns: ingestion_date, file_name, file_version")
            
            # Force Net Revenue read as string to prevent date typing
            w2a_rev_check = map_existing_columns(df, [CAPEX_NUMERIC_COLUMN])
            if CAPEX_NUMERIC_COLUMN in w2a_rev_check:
                rev_col = w2a_rev_check[CAPEX_NUMERIC_COLUMN]
                try:
                    temp_df = pd.read_excel(xls, sheet_name=actual, engine="openpyxl", dtype={rev_col: str})
                    df[rev_col] = temp_df[rev_col]
                    if verbose:
                        print(f"[Capex] Force-read '{rev_col}' as string to prevent date auto-conversion.")
                except Exception as e:
                    if verbose:
                        print(f"[warning] Could not force-read '{rev_col}' as string: {e}")

        # Scrub "Unnamed..." cells
        df = scrub_unnamed_cells(df)

        # Remove "Unnamed: N" columns for Project Economics
        if is_proj:
            df = remove_unnamed_columns(df, verbose=verbose, sheet_name=actual)

        df_out = df.copy()

        # Process sheet
        if is_capex:
            w2a_rev = map_existing_columns(df_out, [CAPEX_NUMERIC_COLUMN])
            if CAPEX_NUMERIC_COLUMN in w2a_rev:
                rev_col = w2a_rev[CAPEX_NUMERIC_COLUMN]
                if verbose:
                    print(f"[Capex] Processing '{rev_col}' as numeric (protected from date conversion)")
                df_out[rev_col] = _to_numeric_preserve_original(
                    df_out[rev_col],
                    excel_origin=excel_origin,
                    verbose=verbose,
                    label=f"{actual}.{rev_col}",
                )
            else:
                if verbose:
                    print(f"[warning] '{actual}' missing numeric col: {CAPEX_NUMERIC_COLUMN}")

            w2a_dates = map_existing_columns(df_out, CAPEX_DATE_COLUMNS)
            if verbose:
                missing = [c for c in CAPEX_DATE_COLUMNS if c not in w2a_dates]
                if missing:
                    print(f"[warning] '{actual}' missing Capex date cols: {', '.join(missing)}")

            for col in w2a_dates.values():
                if CAPEX_NUMERIC_COLUMN in w2a_rev and col == w2a_rev[CAPEX_NUMERIC_COLUMN]:
                    continue
                df_out[col] = _coerce_series_to_date_forced_allow_serials(
                    df_out[col],
                    date_fmt=date_fmt,
                    prefer_dayfirst=prefer_dayfirst,
                    excel_origin=excel_origin,
                    verbose=verbose,
                    label=f"{actual}.{col}",
                )

        elif is_proj:
            w2a_dates = map_existing_columns(df_out, PROJECT_ECON_DATE_COLUMNS)
            if verbose:
                missing = [c for c in PROJECT_ECON_DATE_COLUMNS if c not in w2a_dates]
                if missing:
                    print(f"[warning] '{actual}' missing Project Econ date cols: {', '.join(missing)}")
            for col in w2a_dates.values():
                df_out[col] = _coerce_series_to_date_forced_allow_serials(
                    df_out[col],
                    date_fmt=date_fmt,
                    prefer_dayfirst=prefer_dayfirst,
                    excel_origin=excel_origin,
                    verbose=verbose,
                    label=f"{actual}.{col}",
                )

        elif is_rating:
            for col in df_out.columns:
                df_out[col] = _coerce_series_to_date_strict_no_numeric(
                    df_out[col],
                    date_fmt=date_fmt,
                    prefer_dayfirst=prefer_dayfirst,
                    verbose=verbose,
                    label=f"{actual}.{col}",
                )
            if verbose:
                print(f"[Rating Analysis] Converted date/timestamp cells to date-only across all columns.")

        else:
            cols_to_format = [c for c in df_out.columns if is_date_header(c)]
            if verbose:
                if cols_to_format:
                    print(f"[date headers] {actual}: {', '.join(map(str, cols_to_format))}")
                else:
                    print(f"[no date headers] {actual}")
            for col in cols_to_format:
                df_out[col] = _coerce_series_to_date_strict_no_numeric(
                    df_out[col],
                    date_fmt=date_fmt,
                    prefer_dayfirst=prefer_dayfirst,
                    verbose=verbose,
                    label=f"{actual}.{col}",
                )

        # Save CSV with base filename and version
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename with base name and version
        if is_capex:
            output_filename = f"{base_filename}_Capex and Audit deals.csv"
        else:
            output_filename = f"{base_filename}_{sanitize_filename(actual)}.csv"
            
        out_path = out_dir / output_filename
        written = safe_to_csv(df_out, out_path, index=False, encoding="utf-8-sig", na_rep="")
        print(f"Saved: {written}")
        
        # Store the path for later processing
        csv_files[actual] = written
        exported += 1

    if exported == 0:
        raise SystemExit("No sheets were exported. Check sheet names and try again.")
    
    return csv_files

# =============================
# Script 2 - Rating Analysis Transformation
# =============================

def normalize_to_yyyy_mm_dd(date_like):
    """Normalize many date shapes to 'YYYY-MM-DD'."""
    if pd.isna(date_like):
        return None
    s = str(date_like).strip()
    s = re.sub(r'[\u00A0\u200B]', '', s)

    if re.fullmatch(r'\d{4}', s):
        return f"{s}-01-01"

    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', s):
        return s
    if re.fullmatch(r'\d{4}/\d{2}/\d{2}', s):
        return s.replace('/', '-')
    if re.fullmatch(r'\d{4}\.\d{2}\.\d{2}', s):
        return s.replace('.', '-')

    for kwargs in (dict(dayfirst=False, yearfirst=True),
                   dict(dayfirst=True,  yearfirst=True)):
        try:
            dt_obj = pd.to_datetime(s, errors='raise', **kwargs)
            if getattr(dt_obj, 'tz', None) is not None:
                dt_obj = dt_obj.tz_convert(None) if hasattr(dt_obj, 'tz_convert') else dt_obj.tz_localize(None)
            return dt_obj.strftime('%Y-%m-%d')
        except Exception:
            pass

    return s

def format_annual_dates(dates_list):
    """Convert annual dates to ISO date."""
    formatted = []
    for date_info in dates_list:
        label = str(date_info['label']).strip()
        if re.fullmatch(r'\d{4}', label):
            date_info = dict(date_info)
            date_info['label'] = f"{label}-01-01"
        formatted.append(date_info)
    return formatted

def format_date_for_output(date_str):
    """Convert to strict 'YYYY-MM-DD'."""
    try:
        out = normalize_to_yyyy_mm_dd(date_str)
        return out if out is not None else str(date_str)
    except Exception:
        return str(date_str)

def format_report_date(date_str):
    """Normalize report date to 'YYYY-MM-DD'."""
    try:
        out = normalize_to_yyyy_mm_dd(date_str)
        return out if out is not None else str(date_str)
    except Exception:
        return str(date_str)

def detect_dates_from_header(header_row):
    """Dynamically detect date columns from header row"""
    dates = []
    for col_idx, cell in enumerate(header_row):
        if pd.isna(cell):
            continue
        cell_str = str(cell).strip()
        if (re.match(r'\d{1,2}/\d{1,2}/\d{4}', cell_str) or
            re.match(r'\d{4}-\d{2}-\d{2}', cell_str) or
            re.match(r'\d{4}/\d{2}/\d{2}', cell_str) or
            re.match(r'\d{4}\.\d{2}\.\d{2}', cell_str) or
            re.match(r'^\d{4}$', cell_str)):
            dates.append({'index': col_idx, 'label': cell_str})
    return dates

def get_current_year():
    """Get current year for dynamic table end detection"""
    return dt.datetime.now().year

def find_table_end_column(header_row):
    """Find the end column of the table"""
    max_col = len(header_row) - 1
    for col_idx in range(len(header_row) - 1, -1, -1):
        if pd.notna(header_row[col_idx]) and str(header_row[col_idx]).strip() != '':
            return col_idx
    return max_col

def find_table_sections(df):
    """Find all table sections and their configurations dynamically."""
    sections = []

    table_categories = {
        'IRR excludes Capex and Audit deals': {
            'source_tables': [
                'IRR by Tech',
                'IRR by Credit Rating (ExcludingEquipment Finance)',
                'IRR by Deal Type',
                'IRR by Customer'
            ]
        },
        'NTP excludes Capex and Audit deals': {
            'source_tables': [
                'NTP by Tech',
                'NTP by Credit Rating (ExcludingEquipment Finance)',
                'NTP by Deal Type',
                'NTP by Customer'
            ]
        },
        'XNPV excludes Capex and Audit deals': {
            'source_tables': [
                'XNPV (%) by Tech',
                'XNPV(%) by Credit Rating (ExcludingEquipment Finance)',
                'XNPV (%) by Deal Type',
                'XNPV($) by Customer'
            ]
        }
    }

    for row_idx in range(len(df)):
        for col_idx in range(len(df.columns)):
            if pd.isna(df.iloc[row_idx, col_idx]):
                continue

            cell_value = str(df.iloc[row_idx, col_idx]).strip()

            for table_category, config in table_categories.items():
                if cell_value in config['source_tables']:
                    section = {
                        'table_category': table_category,
                        'source_table_name': cell_value,
                        'header_row': row_idx,
                        'category_col': col_idx,
                        'data_start_col': col_idx + 1
                    }

                    if 'Tech' in cell_value:
                        section['category_type'] = 'Tech'
                    elif 'Credit Rating' in cell_value:
                        section['category_type'] = 'Credit Rating'
                    elif 'Deal Type' in cell_value:
                        section['category_type'] = 'Deal Type'
                    elif 'Customer' in cell_value:
                        section['category_type'] = 'Customer'
                    else:
                        section['category_type'] = 'Category'

                    section['is_percent'] = False

                    date_header_row = row_idx + 2
                    if date_header_row < len(df):
                        section['dates'] = detect_dates_from_header(df.iloc[date_header_row])
                        section['data_start_row'] = date_header_row + 1
                        section['data_end_col'] = find_table_end_column(df.iloc[date_header_row])
                    else:
                        section['dates'] = []
                        section['data_start_row'] = row_idx + 3
                        section['data_end_col'] = len(df.columns) - 1

                    sections.append(section)
                    print(f"Found table: {cell_value} at row {row_idx}, col {col_idx}")
                    print(f"   Data columns: {section['data_start_col']} to {section['data_end_col']}")
                    print(f"   Found {len(section['dates'])} date columns")

    return sections

def find_table_end_row(df, start_row, category_col):
    """Find the end row of a table section"""
    for row_idx in range(start_row + 1, min(start_row + 100, len(df))):
        if row_idx >= len(df):
            return row_idx - 1

        current_cell = df.iloc[row_idx, category_col] if category_col < len(df.iloc[row_idx]) else None

        if pd.isna(current_cell) or str(current_cell).strip() == '':
            empty_count = 0
            for check_idx in range(row_idx, min(row_idx + 3, len(df))):
                check_cell = df.iloc[check_idx, category_col] if category_col < len(df.iloc[check_idx]) else None
                if pd.isna(check_cell) or str(check_cell).strip() == '':
                    empty_count += 1
            if empty_count >= 2:
                return row_idx - 1

        if (pd.notna(current_cell) and
            any(keyword in str(current_cell) for keyword in ['IRR by', 'NTP by', 'XNPV'])):
            return row_idx - 1

    return min(start_row + 50, len(df) - 1)

def clean_and_convert_value(value):
    """Clean and convert raw cell to float if possible."""
    if pd.isna(value) or value in ['', '-']:
        return None

    try:
        if isinstance(value, str):
            clean_value = value.replace('%', '').replace(',', '').replace('$', '').strip()
            if clean_value and clean_value != '-':
                return float(clean_value)
        else:
            return float(value)
    except (ValueError, TypeError):
        return None

    return None

def normalize_numeric_value(cleaned_value, is_percent):
    """Output values kept as-is (no division by 100 for percent values)"""
    return cleaned_value

def transform_rating_analysis_dynamic_columns(csv_file_path, base_filename, ingestion_timestamp=None):
    """Transform Rating Analysis CSV (Script 2 functionality)"""
    if ingestion_timestamp is None:
        ingestion_timestamp = dt.datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')

    print(f"Reading CSV file: {csv_file_path}")

    try:
        df = pd.read_csv(csv_file_path, header=None, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(csv_file_path, header=None, encoding='latin-1')
        except:
            df = pd.read_csv(csv_file_path, header=None, encoding='utf-8', engine='python')

    file_name = base_filename + ".xlsm"
    file_version = 1

    raw_report_date = df.iloc[1, 3] if len(df) > 1 and pd.notna(df.iloc[1, 3]) else 'Unknown'
    report_date = format_report_date(raw_report_date)
    print(f"Report date: {raw_report_date} -> {report_date}")
    print(f"Input file dimensions: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"File name: {file_name}")
    print(f"File version: {file_version}")

    print("\n🔍 Scanning for table sections...")
    sections = find_table_sections(df)
    print(f"✅ Found {len(sections)} table sections")

    result_data = []
    total_rows_processed = 0
    total_values_processed = 0

    for section in sections:
        print(f"\n📊 Processing: {section['source_table_name']}")
        print(f"   Category: {section['category_type']}, Percent metric: {section['is_percent']}")

        if not section['dates']:
            print(f"   ⚠️  No date columns detected in this table")
            continue

        section['dates'] = format_annual_dates(section['dates'])

        print(f"   📅 Found {len(section['dates'])} date columns")
        print(f"   📊 Data range: columns {section['data_start_col']} to {section['data_end_col']}")

        table_end_row = find_table_end_row(df, section['data_start_row'], section['category_col'])
        print(f"   📏 Table rows: {section['data_start_row']} to {table_end_row}")

        rows_processed = 0
        values_processed = 0

        for row_idx in range(section['data_start_row'], table_end_row + 1):
            if row_idx >= len(df):
                break

            row_data = df.iloc[row_idx]

            if len(row_data) <= section['category_col']:
                continue

            category_value = row_data[section['category_col']]

            if (pd.isna(category_value) or
                str(category_value).strip() in ['', 'Tech', 'Credit Rating', 'Deal Type', 'Customer']):
                continue

            if str(category_value).strip() == '':
                break

            for date_info in section['dates']:
                if date_info['index'] < section['data_start_col'] or date_info['index'] > section['data_end_col']:
                    continue
                if date_info['index'] >= len(row_data):
                    continue

                raw_value = row_data[date_info['index']]
                cleaned_value = clean_and_convert_value(raw_value)
                normalized_value = normalize_numeric_value(cleaned_value, section['is_percent'])

                formatted_date = format_date_for_output(date_info['label'])

                final_value = 0 if normalized_value is None else normalized_value

                result_data.append({
                    'report_as_of': report_date,
                    'ingestion_date': ingestion_timestamp,
                    'file_name': file_name,
                    'file_version': file_version,
                    'table_category': section['table_category'],
                    'source_table_name': section['source_table_name'],
                    'category': section['category_type'],
                    'subcategory': str(category_value).strip(),
                    'date': formatted_date,
                    'value': final_value
                })

                values_processed += 1

            rows_processed += 1

        print(f"   📊 Processed {rows_processed} rows, {values_processed} values")
        total_rows_processed += rows_processed
        total_values_processed += values_processed

    result_df = pd.DataFrame(result_data)

    if len(result_df) > 0:
        result_df['date'] = result_df['date'].apply(normalize_to_yyyy_mm_dd)
        result_df['report_as_of'] = result_df['report_as_of'].apply(normalize_to_yyyy_mm_dd)

        result_df['date'] = pd.to_datetime(result_df['date'], errors='coerce').dt.normalize()
        result_df['report_as_of'] = pd.to_datetime(result_df['report_as_of'], errors='coerce').dt.normalize()

        initial_count = len(result_df)
        result_df = result_df.drop_duplicates(subset=[
            'report_as_of', 'table_category', 'source_table_name',
            'category', 'subcategory', 'date'
        ], keep='first')
        duplicates_removed = initial_count - len(result_df)

        result_df = result_df.sort_values(['table_category', 'source_table_name', 'category', 'subcategory', 'date'])
        result_df = result_df.reset_index(drop=True)
    else:
        duplicates_removed = 0

    print(f"\n📈 OVERALL PROCESSING SUMMARY:")
    print(f"   • Total rows processed: {total_rows_processed:,}")
    print(f"   • Total values extracted: {total_values_processed:,}")
    print(f"   • Duplicates removed: {duplicates_removed:,}")
    print(f"   • Final output records: {len(result_df):,}")

    return result_df

# =============================
# Script 3 - Project Economics Processing
# =============================

def convert_date_to_standard_format(date_str):
    """Convert any date format to YYYY-MM-DD format"""
    date_str = str(date_str).strip()

    date_patterns = [
        r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})",
    ]

    for pattern in date_patterns:
        match = re.search(pattern, date_str)
        if match:
            year, month, day = match.groups()
            month = month.zfill(2)
            day = day.zfill(2)
            return f"{year}-{month}-{day}"

    try:
        date_obj = pd.to_datetime(date_str, errors='coerce')
        if not pd.isna(date_obj):
            return date_obj.strftime('%Y-%m-%d')
    except Exception:
        pass

    return ""

def format_date_columns(df, date_columns):
    """Convert specific date columns to YYYY-MM-DD format"""
    for col in date_columns:
        if col in df.columns:
            print(f"Processing date column: {col}")
            df[col] = df[col].apply(lambda x: convert_date_to_standard_format(x) if pd.notna(x) and str(x).strip() != '' else "")
        else:
            print(f"Warning: Date column '{col}' not found in dataframe")

    return df

PCT_NAME_TOKENS = ("%", "percent", "irr", "xirr", "margin", "rate", "apr", "roi")

def _looks_european_decimal(s: str) -> bool:
    """Heuristic: decide if a string likely uses ',' as decimal separator."""
    s = s.strip()
    if "," in s and "." in s:
        return s.rfind(",") > s.rfind(".")
    if "," in s and "." not in s:
        tail = s.split(",")[-1]
        return tail.isdigit() and 1 <= len(tail) <= 2
    return False

def _clean_numeric_string(s: str) -> tuple[str, bool]:
    """Clean numeric string and detect parentheses negatives"""
    if s is None:
        return "", False
    s = str(s).strip()
    if s == "" or s.lower() in {"nan", "na", "n/a", "null", "-", "—", "–"}:
        return "", False

    paren_negative = s.startswith("(") and s.endswith(")")
    if paren_negative:
        s = s[1:-1].strip()

    s = s.replace("−", "-").replace("—", "-").replace("–", "-")

    s_no_curr = re.sub(r"[^\d\.,\-eE]", "", s)

    if _looks_european_decimal(s_no_curr):
        s_no_curr = s_no_curr.replace(".", "").replace(",", ".")
    else:
        s_no_curr = s_no_curr.replace(",", "")

    m = re.match(r"^-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?$", s_no_curr)
    cleaned = s_no_curr if m else re.sub(r"[^0-9eE\.\-]", "", s_no_curr)

    return cleaned, paren_negative

def _coerce_numeric_series(raw: pd.Series, col_name: str) -> pd.Series:
    """Cleans and converts a Series to float dtype."""
    pct_sign_present = raw.astype(str).str.contains(r"%", na=False).mean() > 0

    cleaned_list = []
    neg_flags = []
    for v in raw:
        s = "" if pd.isna(v) else str(v)
        cs, neg = _clean_numeric_string(s)
        cleaned_list.append(cs)
        neg_flags.append(neg)

    cleaned = pd.Series(cleaned_list, index=raw.index)
    nums = pd.to_numeric(cleaned, errors="coerce")

    if any(neg_flags):
        nums.loc[pd.Series(neg_flags, index=raw.index).fillna(False)] *= -1

    name_hint = any(tok in col_name.lower() for tok in PCT_NAME_TOKENS)
    non_null = nums.dropna()
    if len(non_null) == 0:
        return nums.astype(float)

    prop_between_1_100 = ((non_null >= 1) & (non_null <= 100)).mean()
    prop_gt_100 = (non_null > 100).mean()
    median_val = float(non_null.median())

    should_divide = (
        (name_hint or pct_sign_present) and
        prop_gt_100 < 0.05 and
        (prop_between_1_100 >= 0.20 or (1 <= median_val <= 100))
    )

    if should_divide:
        nums = nums / 100.0

    return nums.astype(float)

def clean_numeric_value(v):
    """Single-value cleaner for use inside row loops."""
    if pd.isna(v) or (isinstance(v, str) and v.strip().lower() in {"", "nan", "na", "n/a", "null", "-", "—", "–"}):
        return 0.0
    cs, neg = _clean_numeric_string(str(v))
    x = pd.to_numeric(cs, errors="coerce")
    if pd.isna(x):
        return 0.0
    if neg:
        x = -x
    return float(x)

def format_numeric_columns(df, numeric_columns):
    """Convert specific columns to true numeric dtype with robust cleaning."""
    integer_columns = {
        'NTP Contract: Contract Term (Months)',
        'LOA Contract: Contract Term (Months)'
    }
    
    for col in numeric_columns:
        if col in df.columns:
            print(f"Processing numeric column: {col}")
            
            if col in integer_columns:
                print(f"  Converting {col} directly to integer")
                integer_values = []
                for value in df[col]:
                    cleaned_float = clean_numeric_value(value)
                    if pd.isna(cleaned_float) or cleaned_float == 0.0:
                        integer_values.append(np.nan)
                    else:
                        integer_values.append(int(round(cleaned_float)))
                
                df[col] = integer_values
                df[col] = df[col].astype('Int64')
            else:
                df[col] = _coerce_numeric_series(df[col], col)
            
            non_null = df[col].dropna()
            if len(non_null) > 0:
                print(
                    f"  dtype={df[col].dtype}, min={non_null.min():.6g}, median={non_null.median():.6g}, max={non_null.max():.6g}"
                )
            else:
                print(f"  dtype={df[col].dtype}, all values are NaN/empty after cleaning")
        else:
            print(f"Warning: Numeric column '{col}' not found in dataframe")

    return df

def convert_mapped_integer_columns(df):
    """Convert specific mapped column names to integers after column mapping"""
    integer_columns = [
        'ntp_contract_contract_term_months',
        'loa_contract_contract_term_months'
    ]
    
    for col in integer_columns:
        if col in df.columns:
            print(f"Force converting {col} to strict integer")
            try:
                # First convert to numeric, handling errors
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Then convert to Int64 (nullable integer)
                df[col] = df[col].astype('Int64')
                print(f"  ✓ Successfully converted {col} to Int64")
            except Exception as e:
                print(f"  ⚠ Could not convert {col} to integer: {e}")
                # Keep as float if conversion fails
                df[col] = df[col].astype(float)
    
    return df

def safe_write_csv(df, filename, float_format=None):
    """Safely write DataFrame to CSV with error handling"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df.to_csv(filename, index=False, float_format=float_format)
            print(f"Successfully wrote: {filename}")
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                print(f"Permission denied for {filename}. Please close the file if open in Excel. Retrying in 2 seconds...")
                import time as _t
                _t.sleep(2)
            else:
                print(f"ERROR: Cannot write to {filename}. Please close the file in Excel or other programs and try again.")
                return False
        except Exception as e:
            print(f"Error writing {filename}: {e}")
            return False
    return False

def get_cash_flow_column_mapping():
    """Define the column mapping for Project Economics Cash Flow sheet"""
    return {
        'Opportunity Record ID': 'opportunity_record_id',
        'Parent Account of Proj Oppty: Account Name': 'parent_account_of_proj_oppty_account_name', 
        'Opportunity Name': 'opportunity_name',
        'Flow Name': 'flow_name',
        'Date': 'date',
        'Value': 'value'
    }

def get_details_column_mapping():
    """Define the column mapping for Project Economics Details sheet"""
    return {
        'Opportunity Record ID': 'opportunity_record_id',
        'Parent Account of Proj Oppty: Account Name': 'parent_account_of_proj_oppty_account_name',
        'Opportunity Name': 'opportunity_name',
        'Actual NTP Date': 'actual_ntp_date',
        'LOA Date': 'loa_date',
        'Commercial Operation Date': 'commercial_operation_date',
        'Stage': 'stage',
        'Opportunity Type': 'opportunity_type',
        'NTP Contract Value (converted)': 'ntp_contract_value_converted',
        'LOA Contract Value (converted)': 'loa_contract_value_converted',
        'NTP Contract: Contract Term (Months)': 'ntp_contract_contract_term_months',
        'LOA Contract: Contract Term (Months)': 'loa_contract_contract_term_months',
        'Total Net Cost (converted)': 'total_net_cost_converted',
        'Deal Type': 'deal_type',
        'O&M Cost': 'o_m_cost',
        'Billing Structure': 'billing_structure',
        'LOA Signed (F)': 'loa_signed_f',
        'XNPV margin Salesforce': 'xnpv_margin_salesforce',
        'Customer Parent Opportunity': 'customer_parent_opportunity',
        'NTP Contract Created Date': 'ntp_contract_created_date',
        'Industry': 'industry',
        'Formal Credit rating': 'formal_credit_rating',
        'PV Revenue (PV of all Cash Inflow) (converted)': 'pv_revenue_pv_of_all_cash_inflow_converted',
        'NPV Project Cash Flow (value) (converted)': 'npv_project_cash_flow_value_converted',
        'NTP Contract: IRR %': 'ntp_contract_irr_percent',
        'Opportunity Owner': 'opportunity_owner',
        'NTP Contract: Partner Fee (converted)': 'ntp_contract_partner_fee_converted',
        'Upfront Referral Cost (converted)': 'upfront_referral_cost_converted',
        'Superseding NTP Date': 'superseding_ntp_date',
        'QC XIRR': 'qc_xirr',
        'QC NPV margin': 'qc_npv_margin',
        'Recurring Cost': 'recurring_cost',
        'One time Cost': 'one_time_cost',
        'Est. Monthly Revenue (NTP)': 'est_monthly_revenue_ntp',
        'Back calculated Monthly net cash flow': 'back_calculated_monthly_net_cash_flow',
        'Back calculated Monthly cash inflow': 'back_calculated_monthly_cash_inflow',
        'Actual/Projected LOA date': 'actual_projected_loa_date',
        'LOA Month end date': 'loa_month_end_date',
        'COD Month End Date': 'cod_month_end_date',
        'NTP Contract End Date': 'ntp_contract_end_date',
        'NTP Month End Date': 'ntp_month_end_date',
        'Credit Rating': 'credit_rating',
        'Consider': 'consider',
        'Check this recod': 'check_this_recod',
        'XIRR (NTP)': 'xirr_ntp',
        'XNPV Margin % (NTP)': 'xnpv_margin_percent_ntp',
        'XNPV Net Cash Flow (NTP)': 'xnpv_net_cash_flow_ntp',
        'XNPV Cash Inflow (NTP)': 'xnpv_cash_inflow_ntp',
        'Gross Margin': 'gross_margin',
        'Gross Margin%': 'gross_margin_percent',
        'Start Column - Net Cash Flow (NTP)': 'start_column_net_cash_flow_ntp',
        'End Column - Net Cash Flow (NTP)': 'end_column_net_cash_flow_ntp',
        'Start Column - Cash Inflow (NTP)': 'start_column_cash_inflow_ntp',
        'End Column - Cash Inflow (NTP)': 'end_column_cash_inflow_ntp'
    }

def add_common_columns(df, original_xlsm_filename):
    """Add common columns to dataframe"""
    df['ingestion_date'] = dt.datetime.now().strftime('%m/%d/%Y  %I:%M:%S %p')
    # normalize to bare filename (no directories), so it matches transformed_rating_analysis.csv exactly
    df['file_name'] = Path(original_xlsm_filename).name
    df['file_version'] = 1
    return df


def map_columns(df, column_mapping):
    """Map dataframe columns according to the provided mapping"""
    existing_mapping = {old: new for old, new in column_mapping.items() if old in df.columns}
    df = df.rename(columns=existing_mapping)
    
    missing_columns = set(column_mapping.keys()) - set(existing_mapping.keys())
    if missing_columns:
        print(f"Warning: The following columns were not found and will not be included: {missing_columns}")
    
    return df

def process_project_economics(input_file, base_filename):
    """Process Project Economics CSV into two files (Script 3 functionality)"""
    # Create output file names with base filename and version
    details_output_file = f"{base_filename}_Project Economics Details.csv"
    cash_flow_output_file = f"{base_filename}_Project Economics Cash Flow.csv"
    
    # Get the original XLSM filename
    original_xlsm_filename = base_filename + ".xlsm"

    try:
        df = pd.read_csv(input_file)
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding='latin-1')

    print(f"Original dataset shape: {df.shape}")

    details_columns = []
    cash_flow_columns = []
    cash_inflow_columns = []

    cash_flow_marker_found = False
    cash_inflow_marker_found = False

    for col in df.columns:
        col_str = str(col)
        if "NTP (Net Cash Flow)->" in col_str:
            cash_flow_marker_found = True
            continue
        elif "NTP (Cash Inflow)->" in col_str:
            cash_inflow_marker_found = True
            continue

        if not cash_flow_marker_found and not cash_inflow_marker_found:
            details_columns.append(col)
        elif cash_flow_marker_found and not cash_inflow_marker_found:
            cash_flow_columns.append(col)
        else:
            cash_inflow_columns.append(col)

    print(f"Details columns: {len(details_columns)}")
    print(f"Cash flow columns: {len(cash_flow_columns)}")
    print(f"Cash inflow columns: {len(cash_inflow_columns)}")

    project_details = df[details_columns].copy()

    date_columns_to_format = [
        "Actual NTP Date",
        "LOA Date",
        "Commercial Operation Date", 
        "LOA Signed (F)",
        "NTP Contract Created Date",
        "Superseding NTP Date",
        "Actual/Projected LOA date",
        "LOA Month end date",
        "COD Month End Date",
        "NTP Contract End Date",
        "NTP Month End Date",
    ]

    numeric_columns_to_format = [
        "NTP Contract Value (converted)",
        "LOA Contract Value (converted)",
        "NTP Contract: Contract Term (Months)", 
        "LOA Contract: Contract Term (Months)",
        "Total Net Cost (converted)",
        "XNPV margin Salesforce",
        "PV Revenue (PV of all Cash Inflow) (converted)",
        "NPV Project Cash Flow (value) (converted)",
        "NTP Contract: IRR %",
        "NTP Contract: Partner Fee (converted)",
        "Upfront Referral Cost (converted)",
        "QC XIRR",
        "QC NPV margin",
        "Recurring Cost",
        "One time Cost",
        "Est. Monthly Revenue (NTP)",
        "Back calculated Monthly net cash flow",
        "Back calculated Monthly cash inflow",
        "XIRR (NTP)",
        "XNPV Margin % (NTP)", 
        "XNPV Net Cash Flow (NTP)",
        "XNPV Cash Inflow (NTP)",
        "Gross Margin",
        "Gross Margin%",
    ]

    print("\nFormatting date columns in Project Economics Details...")
    project_details = format_date_columns(project_details, date_columns_to_format)

    print("\nFormatting numeric columns in Project Economics Details...")
    project_details = format_numeric_columns(project_details, numeric_columns_to_format)

    all_opportunity_ids = []
    cash_flow_data = []

    total_rows = len(df)
    print(f"Processing {total_rows} rows...")

    for idx, row in df.iterrows():
        opportunity_id = row.get('Opportunity Record ID')
        parent_account = row.get('Parent Account of Proj Oppty: Account Name')
        opportunity_name = row.get('Opportunity Name')

        opportunity_id_str = str(opportunity_id) if pd.notna(opportunity_id) else f"ROW_{idx+1}_MISSING_ID"
        parent_account_str = str(parent_account) if pd.notna(parent_account) else "Unknown"
        opportunity_name_str = str(opportunity_name) if pd.notna(opportunity_name) else "Unknown"

        all_opportunity_ids.append(opportunity_id_str)

        if (idx + 1) % 50 == 0:
            print(f"Row {idx+1}/{total_rows}: ID='{opportunity_id_str}'")

        for col in cash_flow_columns:
            original_date_str = str(col).strip()
            value = row[col]

            formatted_date = convert_date_to_standard_format(original_date_str)
            processed_value = clean_numeric_value(value)

            cash_flow_data.append({
                'Opportunity Record ID': opportunity_id_str,
                'Parent Account of Proj Oppty: Account Name': parent_account_str,
                'Opportunity Name': opportunity_name_str,
                'Flow Name': 'NTP Net Cash Flow',
                'Date': formatted_date,
                'Value': processed_value,
            })

        for col in cash_inflow_columns:
            original_date_str = str(col).strip()
            value = row[col]

            formatted_date = convert_date_to_standard_format(original_date_str)
            processed_value = clean_numeric_value(value)

            cash_flow_data.append({
                'Opportunity Record ID': opportunity_id_str,
                'Parent Account of Proj Oppty: Account Name': parent_account_str,
                'Opportunity Name': opportunity_name_str,
                'Flow Name': 'NTP Cash Inflow',
                'Date': formatted_date,
                'Value': processed_value,
            })

    project_cash_flow = pd.DataFrame(cash_flow_data)

    print("\nEnsuring 'Value' column is numeric in Project Economics Cash Flow...")
    project_cash_flow['Value'] = pd.to_numeric(project_cash_flow['Value'], errors='coerce').astype(float)
    project_cash_flow['Value'] = project_cash_flow['Value'].fillna(0.0)

    project_cash_flow = project_cash_flow.sort_values(['Opportunity Record ID', 'Date', 'Flow Name'])

    print("\nApplying column mappings and adding common columns...")
    
    details_mapping = get_details_column_mapping()
    project_details = map_columns(project_details, details_mapping)
    project_details = convert_mapped_integer_columns(project_details)
    project_details = add_common_columns(project_details, original_xlsm_filename)  # Pass the filename
    
    cash_flow_mapping = get_cash_flow_column_mapping()
    project_cash_flow = map_columns(project_cash_flow, cash_flow_mapping)
    project_cash_flow = add_common_columns(project_cash_flow, original_xlsm_filename)  # Pass the filename

    print("\nWriting output files with numeric formatting...")

    success1 = safe_write_csv(project_details, details_output_file)
    success2 = safe_write_csv(project_cash_flow, cash_flow_output_file, float_format='%.2f')

    if not (success1 and success2):
        print("Failed to write one or more output files. Please check if files are open in Excel.")
        return None, None

    print(f"\nOutput files created:")
    print(f"  Project Economics Details: {details_output_file}")
    print(f"  Project Economics Cash Flow: {cash_flow_output_file}")
    print(f"Project Economics Details: {project_details.shape}")
    print(f"Project Economics Cash Flow: {project_cash_flow.shape}")

    return details_output_file, cash_flow_output_file

# =============================
# Main Combined Processing Function
# =============================

def process_workbook_combined(
    xlsm_path: Path,
    out_dir: Path,
    *,
    date_fmt: str = "%Y-%m-%d",
    prefer_dayfirst: Optional[bool] = None,
    verbose: bool = False,
):
    """
    Combined processing function that:
    1. Exports 3 sheets from XLSM to CSV
    2. Transforms Rating Analysis CSV 
    3. Processes Project Economics CSV into two files
    4. Returns paths to all 4 final output files
    """
    print("=" * 80)
    print("COMBINED WORKBOOK PROCESSING")
    print("=" * 80)
    
    # Extract base filename from input file (without extension)
    base_filename = xlsm_path.stem
    
    # Step 1: Export sheets from XLSM (Script 1)
    print(f"\n📁 STEP 1: Exporting sheets from XLSM workbook: {xlsm_path.name}")
    csv_files = export_workbook(
        xlsm_path,
        out_dir,
        sheets_wanted=None,
        date_fmt=date_fmt,
        prefer_dayfirst=prefer_dayfirst,
        verbose=verbose,
        base_filename=base_filename
    )
    
    # Step 2: Transform Rating Analysis CSV (Script 2)
    print("\n📊 STEP 2: Transforming Rating Analysis CSV...")
    rating_analysis_path = None
    for sheet_name, file_path in csv_files.items():
        if "Rating Analysis" in sheet_name:
            rating_analysis_path = file_path
            break
    
    if rating_analysis_path and rating_analysis_path.exists():
        transformed_rating = transform_rating_analysis_dynamic_columns(
            str(rating_analysis_path), 
            base_filename
        )
        
        # Save transformed rating analysis with base filename and version
        rating_output_path = out_dir / f"{base_filename}_rating_analysis_sheet.csv"
        
        df_out = transformed_rating.copy()
        for c in ['report_as_of', 'date']:
            if not pd.api.types.is_datetime64_any_dtype(df_out[c]):
                df_out[c] = pd.to_datetime(df_out[c], errors='coerce')
        
        df_out.to_csv(rating_output_path, index=False, float_format='%.17g', date_format='%Y-%m-%d')
        print(f"✅ Saved transformed Rating Analysis: {rating_output_path}")
    else:
        print("❌ Rating Analysis CSV not found for transformation")
        rating_output_path = None
    
    # Step 3: Process Project Economics CSV (Script 3)
    print("\n💼 STEP 3: Processing Project Economics CSV...")
    project_economics_path = None
    for sheet_name, file_path in csv_files.items():
        if "Project Economics" in sheet_name:
            project_economics_path = file_path
            break
    
    if project_economics_path and project_economics_path.exists():
        details_path, cash_flow_path = process_project_economics(
            str(project_economics_path), 
            base_filename=str(out_dir / base_filename)
        )
        
        if details_path and cash_flow_path:
            print(f"✅ Saved Project Economics Details: {details_path}")
            print(f"✅ Saved Project Economics Cash Flow: {cash_flow_path}")
        else:
            print("❌ Failed to process Project Economics CSV")
            details_path = cash_flow_path = None
    else:
        print("❌ Project Economics CSV not found for processing")
        details_path = cash_flow_path = None
    
    # Step 4: Identify Capex file path
    capex_path = None
    for sheet_name, file_path in csv_files.items():
        if any(alias in sheet_name for alias in CAPEX_SHEET_ALIASES):
            capex_path = file_path
            break
    
    # Return all final output file paths
    final_files = {
        'capex': capex_path,
        'rating_analysis': rating_output_path,
        'project_economics_details': Path(details_path) if details_path else None,
        'project_economics_cash_flow': Path(cash_flow_path) if cash_flow_path else None
    }
    
    print("\n🎉 PROCESSING COMPLETE!")
    print("=" * 80)
    print("FINAL OUTPUT FILES:")
    for file_type, file_path in final_files.items():
        status = "✅" if file_path and file_path.exists() else "❌"
        print(f"  {status} {file_type}: {file_path}")
    
    return final_files

# =============================
# Frontend UI
# =============================

def main_frontend():
    """Streamlit frontend application"""
    st.set_page_config(page_title="Rating & Margins Report Automation", page_icon="📊", layout="wide")

    st.title("📊 Rating & Margins Report Automation")
    st.caption("Upload Excel macro files for automated processing and transformation")
    # ===== AUTO TOKEN KEEP-ALIVE (refresh while idle) =====
    KEEPALIVE_SECONDS = 60  # adjust to 30–90s if you like

    # Optional toggle in the sidebar
    keepalive = st.sidebar.checkbox(
        "Keep token fresh automatically",
        value=True,
        help="Reruns the app every 60s while idle so the token auto-refreshes."
    )

    # Always check/refresh token on every render
    tok, exp_epoch = ensure_token_in_session_and_localstorage(scope="all-apis")

    # Trigger rerun only when not actively processing
    if keepalive and not st.session_state.get("processing_complete", False):
        if _HAVE_AUTOREFRESH:
            # Soft rerun (recommended)
            st_autorefresh(interval=KEEPALIVE_SECONDS * 1000, key="dbx-keepalive")
        else:
            # Fallback: hard reload with JS (only when tab is visible)
            components.html(f"""
                <script>
                  const sec = {KEEPALIVE_SECONDS} * 1000;
                  if (!window.__dbxKeepAlive) {{
                    window.__dbxKeepAlive = setInterval(() => {{
                      if (document.visibilityState === 'visible') {{
                        window.location.reload();
                      }}
                    }}, sec);
                  }}
                </script>
            """, height=0)
    # ===== /AUTO TOKEN KEEP-ALIVE =====

    # Global CSS for the scrollable, dark-themed log area
    st.markdown(
        """
<style>
.log-window {
    background: #0E1117;
    color: #FAFAFA;
    border: 1px solid #262730;
    border-radius: 5px;
    height: 400px;
    overflow-y: auto;
    padding: 12px;
    font-family: 'Courier New', monospace;
    font-size: 13px;
    line-height: 1.4;
}
.log-window pre {
    margin: 0;
    white-space: pre-wrap;
    word-wrap: break-word;
}
</style>
""",
        unsafe_allow_html=True,
    )

    # Setup directories
    UPLOAD_DIR, LOG_CSV = setup_frontend_directories()

    # Initialize session state for download files
    if 'download_files' not in st.session_state:
        st.session_state.download_files = None
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'uploaded_file_name' not in st.session_state:
        st.session_state.uploaded_file_name = None

    # File upload section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        file = st.file_uploader("Choose an Excel macro file (.xlsm only)", type=["xlsm"], 
                               key="file_uploader")
    
    with col2:
        overwrite = st.checkbox("Allow overwrite if file exists", value=False, key="overwrite_checkbox")
        process_button = st.button("Upload & Process File", type="primary", key="process_button")

    # Terminal output section
    st.subheader("📋 Processing Logs")
    terminal_placeholder = st.empty()
    
    # Initialize terminal output capture with styled, scrollable area
    terminal_output = StreamlitOutput(terminal_placeholder)
    terminal_output.update_display()  # render the empty log window immediately
    
    # Process file when button is clicked
    if process_button and file is not None:
        # Reset session state for new processing
        st.session_state.download_files = None
        st.session_state.processing_complete = False
        st.session_state.uploaded_file_name = None

        # Validate file
        if not file.name.lower().endswith(".xlsm"):
            st.error("Only .xlsm files are allowed.")
            st.stop()

        file_bytes = file.read()
        size_mb = len(file_bytes) / (1024 * 1024)
        if size_mb > MAX_MB:
            st.error(f"File too large: {size_mb:.2f} MB. Max allowed is {MAX_MB} MB.")
            st.stop()

        file_hash = hash_bytes(file_bytes)
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        safe_name = safe_filename(file.name)
        stem, ext = os.path.splitext(safe_name)
        stored_name = safe_name if overwrite else f"{stem}_{timestamp}{ext}"
        target_path = UPLOAD_DIR / stored_name

        # Redirect stdout to our terminal output
        import sys as _sys
        old_stdout = _sys.stdout
        old_stderr = _sys.stderr
        _sys.stdout = terminal_output
        _sys.stderr = terminal_output

        try:
            # Save uploaded file
            with open(target_path, "wb") as f:
                f.write(file_bytes)

            st.success(f"✅ File uploaded successfully! Starting processing...")
            
            # Example: create a requests session with current token (optional)
            # session = requests.Session()
            # if st.session_state.get("dbx_token"):
                # session.headers.update({"Authorization": f"Bearer {st.session_state['dbx_token']}"})

            # Process the file
            print(f"Starting processing of: {safe_name}")
            final_files = process_workbook_combined(
                Path(target_path),
                UPLOAD_DIR,
                date_fmt="%Y-%m-%d",
                verbose=True
            )

            # Log success
            log_row = {
                "event_time": datetime.utcnow().isoformat(sep=" "),
                "filename": safe_name,
                "stored_path": str(target_path),
                "filesize_bytes": len(file_bytes),
                "file_hash": file_hash,
                "status": "SUCCESS",
                "message": f"Generated {sum(1 for f in final_files.values() if f)} output files",
            }
            log_event_csv(LOG_CSV, log_row)

            # Store results in session state
            st.session_state.download_files = final_files
            st.session_state.processing_complete = True
            st.session_state.uploaded_file_name = safe_name

            # Display results
            st.success("🎉 Processing completed successfully!")

        except Exception as e:
            # Log error
            status = "ERROR"
            message = str(e)
            err_row = {
                "event_time": datetime.utcnow().isoformat(sep=" "),
                "filename": safe_name,
                "stored_path": str(target_path),
                "filesize_bytes": len(file_bytes),
                "file_hash": file_hash,
                "status": status,
                "message": message,
            }
            try:
                log_event_csv(LOG_CSV, err_row)
            except Exception:
                pass
            st.error("❌ Processing failed. Check the logs for details.")
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # Restore stdout/stderr
            _sys.stdout = old_stdout
            _sys.stderr = old_stderr
            terminal_output.update_display()

    elif process_button and file is None:
        st.error("Please upload a valid Excel macro file (.xlsm).")

    # Show download buttons persistently after processing
    if st.session_state.processing_complete and st.session_state.download_files:
        st.subheader("📁 Generated Files - Ready for Download")
        
        # Create columns for better layout
        cols = st.columns(2)
        col_idx = 0
        
        for file_type, file_path in st.session_state.download_files.items():
            if file_path and file_path.exists():
                with cols[col_idx]:
                    st.success(f"**{file_type.replace('_', ' ').title()}**")
                    with open(file_path, "rb") as f:
                        # Use a unique key that includes the file_type and uploaded file name
                        download_key = f"download_{file_type}_{st.session_state.uploaded_file_name}"
                        st.download_button(
                            label=f"Download {file_type.replace('_', ' ').title()}",
                            data=f,
                            file_name=file_path.name,
                            mime="text/csv",
                            key=download_key
                        )
                col_idx = (col_idx + 1) % 2  # Alternate between columns
        
        # Database Upload Section
        st.divider()
        st.subheader("🚀 Database Upload")
        
        upload_col1, upload_col2 = st.columns([3, 1])
        
        with upload_col1:
            st.info("""
            **Database Upload Feature** - Coming Soon!
            
            This feature will allow you to upload the processed data directly to the database 
            for further analysis and reporting. The functionality is currently under development.
            """)
        
        with upload_col2:
            # Disabled Upload to Database button
            st.button(
                "📤 Upload to Database", 
                disabled=True,
                help="Database upload functionality is currently disabled - coming soon!",
                key="upload_database_btn"
            )
        
        # Add a clear button to reset the state
        if st.button("Clear Results & Process New File"):
            st.session_state.download_files = None
            st.session_state.processing_complete = False
            st.session_state.uploaded_file_name = None
            st.rerun()

    # Footer
    st.divider()
    st.caption("© 2025 Redaptive Inc. | Rating & Margins (FPNA) Automation")

# =============================
# CLI Backend (for standalone use)
# =============================

def main_cli():
    """CLI entry point for backend processing"""
    parser = argparse.ArgumentParser(
        description="Combined script to process XLSM workbook and generate 4 output CSV files"
    )
    parser.add_argument("input", help="Path to the .xlsm file")
    parser.add_argument("-o", "--outdir", default=None, help="Output directory (default: same as input)")
    parser.add_argument("--date-format", default="%Y-%m-%d",
                        help='strftime format for dates (default: "%Y-%m-%d").')
    parser.add_argument("--dayfirst", action="store_true",
                        help="Prefer day-first parsing for ambiguous strings in targeted columns.")
    parser.add_argument("--verbose", action="store_true", help="Print detailed actions.")
    
    args = parser.parse_args()

    xlsm_path = Path(args.input)
    if not xlsm_path.exists():
        raise SystemExit(f"Input file not found: {xlsm_path}")

    out_dir = Path(args.outdir) if args.outdir else xlsm_path.parent
    prefer_dayfirst = True if args.dayfirst else None

    # Run the combined processing
    final_files = process_workbook_combined(
        xlsm_path,
        out_dir,
        date_fmt=args.date_format,
        prefer_dayfirst=prefer_dayfirst,
        verbose=args.verbose,
    )

    # Summary
    successful_files = sum(1 for path in final_files.values() if path and path.exists())
    print(f"\n📈 SUMMARY: {successful_files}/4 files generated successfully")

# =============================
# Main Entry Point
# =============================

if __name__ == "__main__":
    # Streamlit mode: launched via `streamlit run` or `python -m streamlit run`
    # CLI mode: launched via `python integrated_app.py input.xlsm`
    if len(sys.argv) == 1:
        main_frontend()
    else:
        main_cli()
