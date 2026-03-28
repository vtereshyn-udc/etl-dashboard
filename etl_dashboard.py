"""
ETL Monitor v4.2
- Sidebar навігація
- Uptime воркерів (% успішних запусків)
- Теплова карта
- Алерти в Telegram
- NEW: Сьогодні виджет (24г)
"""

import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime, date, timedelta
import pytz

st.set_page_config(
    page_title="ETL Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

KYIV_TZ = pytz.timezone("Europe/Kyiv")

def now_kyiv():
    return datetime.now(KYIV_TZ)

# ============================================================
# РОЗКЛАД v4.2
# ============================================================
SCHEDULE = [
    (0,  45, "promotions",       None),
    (1,  0,  "rank_tracker",     None),
    (2,  0,  "ads",              30),
    (3,  30, "ledger_summary",   2),
    (3,  35, "ledger_detail",    7),
    (4,  30, "bulk_daily",       None),
    (5,  0,  "all_orders",       7),
    (6,  0,  "ads",              7),
    (6,  30, "brand_analytics",  None),
    (7,  0,  "shipments",        None),
    (7,  30, "fba_inbound",      None),
    (8,  0,  "data_quality",     None),
    (9,  0,  "inventory",        None),
    (9,  20, "sales_traffic",    3),
    (9,  30, "transactions",     7),
    (10, 0,  "ads",              7),
    (10, 5,  "alerts",           None),
    (10, 10, "manage_fba",       None),
    (10, 45, "promotions",       None),
    (11, 0,  "fba_returns",      None),
    (11, 20, "fba_replacements", None),
    (13, 10, "awd_inventory",    None),
    (13, 20, "sales_traffic",    5),
    (13, 40, "all_orders",       2),
    (14, 0,  "ads",              7),
    (14, 10, "transactions",     7),
    (14, 45, "promotions",       None),
    (15, 0,  "inventory",        None),
    (16, 10, "sales_traffic",    7),
    (16, 20, "transactions",     60),
    (16, 30, "manage_fba",       None),
    (17, 10, "awd_inventory",    None),
    (18, 0,  "ads",              7),
    (19, 10, "awd_inventory",    None),
    (19, 45, "promotions",       None),
    (21, 0,  "inventory",        None),
    (22, 0,  "manage_fba",       None),
    (23, 30, "all_orders",       30),
]

TASK_MAP = [
    ("ads",              "api_ad.sp_campaign_api",        "date",          "🎯", "ADS SP"),
    ("ads",              "api_ad.sb_campaign_api",        "date",          "🧲", "ADS SB"),
    ("ads",              "api_ad.sd_campaign_api",        "date",          "📡", "ADS SD"),
    ("inventory",        "csv.fba_inventory",             "snapshot_date", "🏭", "FBA Inventory Health"),
    ("manage_fba",       "csv.manage_fba_inventory",      "date",          "📋", "Manage FBA"),
    ("awd_inventory",    "csv.inventory",                 "date",          "🏢", "AWD + FBA Inventory"),
    ("all_orders",       "spapi.all_orders",              "purchase_date", "🛒", "All Orders"),
    ("shipments",        "csv.fulfilled_shipments",       "shipment_date", "🚚", "Fulfilled Shipments"),
    ("bulk_daily",       "spapi.full_bulk_daily",         "date",          "📦", "Bulk Daily"),
    ("transactions",     "csv.transaction",               "date_time",     "💰", "Transactions"),
    ("fba_returns",      "spapi.fba_returns",             "return_date",   "🔄", "FBA Returns"),
    ("fba_replacements", "csv.replacements",              "shipment_date", "🔁", "FBA Replacements"),
    ("sales_traffic",    "spapi.sales_traffic_report",    "date",          "📊", "Sales & Traffic"),
    ("promotions",       "csv.promotions",                "shipment_date", "🎁", "Promotions"),
    ("rank_tracker",     "amzudc.rank_tracker",           "date",          "🔍", "Rank Tracker"),
    ("ledger_summary",   "spapi.ledger_summary",          "report_date",   "📒", "Ledger Summary"),
    ("ledger_detail",    "spapi.ledger_detail",           "event_date",    "📒", "Ledger Detail"),
    ("brand_analytics",  "brand_analytics.asin",          "reporting_date","📊", "Brand Analytics"),
]

ALERT_THRESHOLDS = {
    "ads":              10,
    "transactions":     14,
    "all_orders":       14,
    "sales_traffic":    14,
    "promotions":       12,
    "inventory":        30,
    "manage_fba":       30,
    "awd_inventory":    30,
    "shipments":        30,
    "fba_returns":      30,
    "fba_replacements": 30,
    "rank_tracker":     30,
    "ledger_summary":   30,
    "ledger_detail":    30,
    "bulk_daily":       30,
    "brand_analytics":  30,
    "data_quality":     30,
}

CUSTOM_THRESHOLDS = {
    "inventory": (28, 52), "manage_fba": (28, 52), "awd_inventory": (28, 52),
    "rank_tracker": (28, 52), "ledger_summary": (28, 52), "ledger_detail": (28, 52),
    "bulk_daily": (28, 52), "shipments": (36, 72), "fba_replacements": (36, 72),
    "brand_analytics": (168, 200),
}

# ============================================================
# THEME
# ============================================================
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True
dark = st.session_state.dark_mode

if dark:
    bg, bg2, bg3 = "#080b12", "#0d1220", "#0f1520"
    border = "#151e30"
    text1, text2, text3, text4 = "#ffffff", "#d0d8e8", "#ffffff", "#6b7e9f"
    row_hover, th_bg = "#0f1724", "#080b12"
    sb_bg = "#0a0e18"
else:
    bg, bg2, bg3 = "#f4f6fb", "#ffffff", "#f8f9fc"
    border = "#e2e8f0"
    text1, text2, text3, text4 = "#0f172a", "#334155", "#0f172a", "#64748b"
    row_hover, th_bg = "#f1f5f9", "#f8f9fc"
    sb_bg = "#f0f2f8"

# ============================================================
# CSS
# ============================================================
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@400;500;600;700&display=swap');
* {{ box-sizing:border-box; }}
.stApp {{ background:{bg} !important; font-family:'Inter',sans-serif; }}
.block-container {{ padding-top:1.5rem !important; max-width:1300px; }}

[data-testid="stSidebar"] .stButton > button {{
    background: transparent !important;
    border: 1px solid rgba(0,0,0,.15) !important;
    color: inherit !important;
    text-align: left !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 8px 12px !important;
    border-radius: 8px !important;
    transition: all .15s !important;
}}
[data-testid="stSidebar"] .stButton > button:hover {{
    border-color: #3b82f6 !important;
    color: #3b82f6 !important;
}}
[data-testid="stSidebar"] .stButton > button[kind="primary"] {{
    background: rgba(59,130,246,.15) !important;
    border-color: #3b82f6 !important;
    color: #3b82f6 !important;
    font-weight: 700 !important;
}}
[data-testid="stSidebar"] {{
    background:{sb_bg} !important;
    border-right:1px solid {border} !important;
    min-width:220px !important;
    max-width:220px !important;
}}
[data-testid="stSidebar"] * {{ color:{text2} !important; }}

.sb-logo {{ padding:16px 16px 8px; }}
.sb-logo img {{ height:26px; filter:{'brightness(0) invert(1)' if dark else 'none'}; opacity:.8; }}
.sb-title {{ font-size:13px; font-weight:700; color:{text1} !important; margin-top:6px; letter-spacing:-.2px; }}
.sb-sub {{ font-size:10px; color:{text4} !important; font-family:JetBrains Mono,monospace; margin-top:2px; }}
.sb-divider {{ border:none; border-top:1px solid {border}; margin:12px 0; }}

.page-header {{ margin-bottom:20px; }}
.page-title {{ font-size:22px; font-weight:700; color:{text1}; letter-spacing:-.3px; }}
.page-sub {{ font-size:12px; color:{text4}; font-family:JetBrains Mono,monospace; margin-top:2px; }}

.metrics-row {{ display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:18px; }}
.metric {{ background:{bg2}; border:1px solid {border}; border-radius:10px; padding:15px 18px; position:relative; overflow:hidden; }}
.metric::after {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; }}
.m-ok::after {{ background:#22c55e; }} .m-warn::after {{ background:#f59e0b; }}
.m-stale::after {{ background:#ef4444; }} .m-total::after {{ background:#3b82f6; }}
.metric-num {{ font-size:36px; font-weight:700; font-family:JetBrains Mono,monospace; line-height:1; }}
.m-ok .metric-num {{ color:#22c55e; }} .m-warn .metric-num {{ color:#f59e0b; }}
.m-stale .metric-num {{ color:#ef4444; }} .m-total .metric-num {{ color:#3b82f6; }}
.metric-lbl {{ font-size:10px; color:{text4}; text-transform:uppercase; letter-spacing:.1em; margin-top:4px; font-weight:600; }}

.etl-wrap {{ background:{bg2}; border:1px solid {border}; border-radius:12px; overflow:hidden; margin-top:10px; }}
.etl-table {{ width:100%; border-collapse:collapse; font-size:13px; }}
.etl-table th {{ background:{th_bg}; color:{text3}; font-size:10px; text-transform:uppercase; letter-spacing:.1em; padding:10px 14px; text-align:left; font-weight:700; border-bottom:1px solid {border}; }}
.etl-table td {{ padding:11px 14px; border-bottom:1px solid {bg3}; vertical-align:middle; }}
.etl-table tr:last-child td {{ border-bottom:none; }}
.etl-table tr:hover td {{ background:{row_hover}; }}
.c-name {{ color:{text1}; font-weight:600; font-size:14px; }}
.c-tbl  {{ color:#7ec8a0; font-family:JetBrains Mono,monospace; font-size:12px; font-weight:500; }}
.c-cnt  {{ color:{text2}; font-family:JetBrains Mono,monospace; font-size:12px; }}
.c-ran  {{ color:#4a9e6b; font-family:JetBrains Mono,monospace; font-size:11px; }}
.c-nxt  {{ color:#5a7a9e; font-family:JetBrains Mono,monospace; font-size:11px; }}
.c-ok {{ color:#22c55e; font-weight:500; }} .c-warn {{ color:#f59e0b; font-weight:500; }}
.c-stale {{ color:#ef4444; font-weight:500; }} .c-empty {{ color:{text4}; }}
.freq-pill {{ font-size:10px; color:{text4}; background:{bg3}; border:1px solid {border}; padding:2px 8px; border-radius:10px; font-family:JetBrains Mono,monospace; white-space:nowrap; }}
.badge {{ display:inline-flex; align-items:center; gap:4px; padding:3px 9px; border-radius:20px; font-size:11px; font-weight:600; white-space:nowrap; }}
.b-ok {{ background:rgba(34,197,94,.1); color:#22c55e; border:1px solid rgba(34,197,94,.25); }}
.b-warn {{ background:rgba(245,158,11,.1); color:#f59e0b; border:1px solid rgba(245,158,11,.25); }}
.b-stale {{ background:rgba(239,68,68,.1); color:#ef4444; border:1px solid rgba(239,68,68,.25); }}
.b-empty {{ background:rgba(100,116,139,.1); color:{text4}; border:1px solid {border}; }}
.dot {{ width:6px; height:6px; border-radius:50%; display:inline-block; }}
.dot-ok {{ background:#22c55e; {'box-shadow:0 0 5px #22c55e;' if dark else ''} }}
.dot-warn {{ background:#f59e0b; {'box-shadow:0 0 5px #f59e0b;' if dark else ''} }}
.dot-stale {{ background:#ef4444; {'box-shadow:0 0 5px #ef4444;' if dark else ''} }}
.dot-empty {{ background:{text4}; }}

.stat-card {{ background:{bg2}; border:1px solid {border}; border-radius:12px; padding:18px 20px; margin-bottom:14px; }}
.stat-card h4 {{ font-size:11px; color:{text4}; text-transform:uppercase; letter-spacing:.1em; margin:0 0 14px; font-weight:700; }}
.alert-row {{ background:rgba(239,68,68,.07); border:1px solid rgba(239,68,68,.2); border-radius:8px; padding:10px 14px; margin-bottom:6px; display:flex; justify-content:space-between; align-items:center; }}
.alert-name {{ color:#ef4444; font-weight:600; font-size:13px; }}
.alert-info {{ color:{text2}; font-size:11px; font-family:JetBrains Mono,monospace; }}

.uptime-row {{ display:flex; align-items:center; gap:12px; margin-bottom:8px; }}
.uptime-label {{ color:{text2}; font-size:12px; font-family:JetBrains Mono,monospace; min-width:130px; }}
.uptime-bar-wrap {{ flex:1; height:8px; background:{"#1a2235" if dark else "#e2e8f0"}; border-radius:4px; overflow:hidden; }}
.uptime-pct {{ font-size:12px; font-weight:600; min-width:44px; text-align:right; font-family:JetBrains Mono,monospace; }}

.hm-wrap {{ overflow-x:auto; padding-bottom:4px; }}
.hm-table {{ border-collapse:separate; border-spacing:2px; font-size:11px; font-family:JetBrains Mono,monospace; }}
.hm-table td.cell {{ width:16px; height:16px; border-radius:3px; }}
.hm-label {{ color:{text2}; padding-right:10px; text-align:right; white-space:nowrap; font-size:11px; min-width:110px; }}
.hm-date-label {{ color:{text4}; font-size:10px; padding:0 2px; text-align:left; }}

.etl-footer {{ text-align:center; font-size:11px; color:{text4}; margin-top:16px; font-family:JetBrains Mono,monospace; }}
#MainMenu,footer,header,.stDeployButton {{ display:none !important; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# DB
# ============================================================
def query(sql, params=None):
    db_url = st.secrets["DATABASE_URL"]
    try:
        conn = psycopg2.connect(
            db_url.replace("postgres://", "postgresql://", 1),
            sslmode='require', connect_timeout=10
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        return None

def table_exists(schema, table):
    r = query("SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)", (schema, table))
    return r and r[0][0]

def get_stats(schema_table, date_col):
    parts = schema_table.split(".")
    schema, table = parts[0], parts[1]
    if not table_exists(schema, table):
        return None, None
    r = query("""
        SELECT reltuples::BIGINT
        FROM pg_class
        WHERE oid = %s::regclass
    """, (f'"{schema}"."{table}"',))
    count = r[0][0] if r else 0
    last_date = None
    if date_col:
        r2 = query(f'SELECT MAX("{date_col}") FROM "{schema}"."{table}"')
        if r2 and r2[0][0]:
            last_date = r2[0][0]
    return count, last_date

# ============================================================
# HELPERS
# ============================================================

def hours_since(last_date):
    if last_date is None:
        return None
    now = datetime.now()
    if isinstance(last_date, date) and not isinstance(last_date, datetime):
        last_dt = datetime(last_date.year, last_date.month, last_date.day)
    else:
        last_dt = last_date.replace(tzinfo=None) if hasattr(last_date, 'tzinfo') and last_date.tzinfo else last_date
    return (now - last_dt).total_seconds() / 3600

def get_status(task_type, last_date):
    h = hours_since(last_date)
    if h is None:
        return "empty"
    if task_type in CUSTOM_THRESHOLDS:
        ok_h, warn_h = CUSTOM_THRESHOLDS[task_type]
    else:
        runs = sum(1 for _, _, t, _ in SCHEDULE if t == task_type)
        if not runs:
            return "unknown"
        ok_h = (24 / runs) * 1.5
        warn_h = ok_h * 2.5
    return "ok" if h <= ok_h else "warn" if h <= warn_h else "stale"

def fmt_last(last_date):
    if last_date is None:
        return "—"
    h = hours_since(last_date)
    if h is None:
        return "—"
    if h < 1:
        return f"{int(h*60)}хв тому"
    elif h < 24:
        return f"{int(h)}г тому"
    elif h < 48:
        return "вчора"
    else:
        return f"{int(h/24)}д тому"

def get_next_run(task_type):
    now = now_kyiv()
    today = now.date()
    candidates = []
    for h, m, t, p in SCHEDULE:
        if t != task_type:
            continue
        scheduled = KYIV_TZ.localize(datetime(today.year, today.month, today.day, h, m, 0))
        if scheduled <= now:
            scheduled += timedelta(days=1)
        candidates.append(scheduled)
    return min(candidates) if candidates else None

def fmt_next(task_type):
    nxt = get_next_run(task_type)
    if not nxt:
        return "—"
    now = now_kyiv()
    diff = (nxt - now).total_seconds()
    h, m = int(diff // 3600), int((diff % 3600) // 60)
    return f"{nxt.strftime('%H:%M')} ({h}г {m:02d}хв)" if h > 0 else f"{nxt.strftime('%H:%M')} ({m}хв)"

def get_runs_per_day(task_type):
    return f"{sum(1 for _, _, t, _ in SCHEDULE if t == task_type)}× / день"

def fmt_ran_at(ran_at):
    if not ran_at:
        return "—"
    now = datetime.now()
    ran = ran_at.replace(tzinfo=None) if hasattr(ran_at, 'tzinfo') and ran_at.tzinfo else ran_at
    diff = (now - ran).total_seconds()
    if diff < 60:
        return f"{int(diff)}с"
    elif diff < 3600:
        return f"{int(diff/60)}хв тому"
    elif diff < 86400:
        return f"{int(diff/3600)}г тому"
    else:
        return f"{int(diff/86400)}д тому"

# ============================================================
# DATA LOADERS
# ============================================================

@st.cache_data(ttl=120)
def load_etl_log():
    r = query("SELECT DISTINCT ON (task_type) task_type, ran_at, rows_saved, elapsed_sec, status FROM public.etl_log ORDER BY task_type, ran_at DESC")
    if not r:
        return {}
    return {row[0]: {"ran_at": row[1], "rows_saved": row[2], "elapsed_sec": row[3], "status": row[4]} for row in r}

@st.cache_data(ttl=120)
def load_etl_log_history(days=30):
    r = query(f"SELECT task_type, ran_at, rows_saved, elapsed_sec, status FROM public.etl_log WHERE ran_at >= NOW() - INTERVAL '{days} days' ORDER BY ran_at DESC")
    if not r:
        return pd.DataFrame()
    df = pd.DataFrame(r, columns=["task_type", "ran_at", "rows_saved", "elapsed_sec", "status"])
    df["ran_at"] = pd.to_datetime(df["ran_at"], utc=True)
    df["date"] = df["ran_at"].dt.tz_convert(KYIV_TZ).dt.date
    df["elapsed_min"] = pd.to_numeric(df["elapsed_sec"], errors="coerce") / 60
    return df

@st.cache_data(ttl=120)
def load_all():
    rows = []
    etl_log = load_etl_log()
    for task_type, schema_table, date_col, icon, label in TASK_MAP:
        count, last_date = get_stats(schema_table, date_col)
        status = get_status(task_type, last_date) if count is not None else "empty"
        rows.append({
            "task": task_type, "icon": icon, "label": label, "table": schema_table,
            "count": count, "last_str": fmt_last(last_date), "next_str": fmt_next(task_type),
            "freq": get_runs_per_day(task_type), "status": status,
            "ran_at": fmt_ran_at(etl_log.get(task_type, {}).get("ran_at")),
        })
    return rows

@st.cache_data(ttl=30)
def load_system_metrics():
    r = query("""
        SELECT cpu_pct, ram_pct, ram_used_mb, ram_total_mb,
               disk_pct, disk_used_gb, disk_total_gb,
               db_connections, db_size_mb,
               run_forever_alive, run_forever_pid, run_forever_uptime_sec,
               running_workers, collected_at
        FROM public.system_metrics
        ORDER BY collected_at DESC LIMIT 1
    """)
    if not r:
        return None
    row = r[0]
    return {
        "cpu": row[0], "ram_pct": row[1], "ram_used_mb": row[2], "ram_total_mb": row[3],
        "disk_pct": row[4], "disk_used_gb": row[5], "disk_total_gb": row[6],
        "db_conn": row[7], "db_size_mb": row[8],
        "alive": row[9], "pid": row[10], "uptime_sec": row[11],
        "workers": row[12], "collected_at": row[13]
    }

@st.cache_data(ttl=300)
def load_table_sizes():
    r = query("""
        SELECT DISTINCT ON (table_name) table_name, size_mb, row_count
        FROM public.system_table_sizes
        ORDER BY table_name, collected_at DESC
    """)
    if not r:
        return []
    return sorted(r, key=lambda x: x[1] or 0, reverse=True)

@st.cache_data(ttl=30)
def load_cpu_history():
    r = query("""
        SELECT collected_at, cpu_pct, ram_pct
        FROM public.system_metrics
        WHERE collected_at >= NOW() - INTERVAL '2 hours'
        ORDER BY collected_at ASC
    """)
    if not r:
        return pd.DataFrame()
    return pd.DataFrame(r, columns=["collected_at", "cpu_pct", "ram_pct"])

@st.cache_data(ttl=300)
def load_db_overview():
    r = query("""
        SELECT
            t.table_schema                                          as schema,
            t.table_name                                            as table_name,
            t.table_schema || '.' || t.table_name                  as full_name,
            COALESCE(s.n_live_tup, 0)                              as row_count,
            COALESCE(
                pg_total_relation_size(
                    quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)
                ) / 1024.0 / 1024.0, 0
            )                                                       as size_mb,
            greatest(s.last_vacuum, s.last_autovacuum,
                     s.last_analyze, s.last_autoanalyze)            as last_activity
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s
            ON s.schemaname = t.table_schema AND s.relname = t.table_name
        WHERE t.table_schema NOT IN (
            'pg_catalog','information_schema','pg_toast','heroku_ext'
        )
        AND t.table_type = 'BASE TABLE'
        ORDER BY size_mb DESC
    """)
    if not r:
        return pd.DataFrame()
    df = pd.DataFrame(r, columns=["schema","table","full_name","row_count","size_mb","last_activity"])
    df["size_mb"]   = pd.to_numeric(df["size_mb"],   errors="coerce").fillna(0).round(2)
    df["row_count"] = pd.to_numeric(df["row_count"],  errors="coerce").fillna(0).astype(int)
    return df

@st.cache_data(ttl=300)
def load_growth_per_day():
    r = query("""
        SELECT task_type,
               ROUND(SUM(rows_saved) / 7.0, 0) as rows_per_day
        FROM public.etl_log
        WHERE ran_at >= NOW() - INTERVAL '7 days'
          AND status = 'ok'
          AND rows_saved > 0
        GROUP BY task_type
    """)
    if not r:
        return {}
    return {row[0]: int(row[1]) for row in r}

@st.cache_data(ttl=30)
def load_pending_queue():
    r = query("""
        SELECT task_type, status, COUNT(*) as cnt,
               MIN(created_at) as oldest
        FROM public.pending_reports
        WHERE status IN ('pending', 'downloading', 'error')
        GROUP BY task_type, status
        ORDER BY status, task_type
    """)
    if not r:
        return []
    return r

@st.cache_data(ttl=30)
def load_collector_health():
    r = query("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'DONE')                        as total_done,
            COUNT(*) FILTER (WHERE status = 'DONE'
                AND updated_at >= NOW() - INTERVAL '24 hours')             as done_today,
            COUNT(*) FILTER (WHERE status = 'pending')                     as pending_now,
            COUNT(*) FILTER (WHERE status = 'error')                       as errors,
            MAX(updated_at) FILTER (WHERE status = 'DONE')                 as last_collected,
            MIN(created_at) FILTER (WHERE status = 'pending')              as oldest_pending
        FROM public.pending_reports
    """)
    if not r:
        return None
    row = r[0]
    return {
        "total_done":     row[0] or 0,
        "done_today":     row[1] or 0,
        "pending_now":    row[2] or 0,
        "errors":         row[3] or 0,
        "last_collected": row[4],
        "oldest_pending": row[5],
    }

SLA_DEADLINES = {
    "rank_tracker":     "10:00",
    "ads":              "10:30",
    "ledger_summary":   "06:00",
    "ledger_detail":    "06:00",
    "bulk_daily":       "07:00",
    "all_orders":       "07:00",
    "brand_analytics":  "08:00",
    "shipments":        "09:00",
    "fba_inbound":      "10:00",
    "data_quality":     "09:00",
    "inventory":        "10:00",
    "sales_traffic":    "11:00",
    "transactions":     "11:00",
    "manage_fba":       "11:00",
    "fba_returns":      "12:00",
    "fba_replacements": "12:00",
    "promotions":       "12:00",
}

@st.cache_data(ttl=60)
def load_sla_status():
    r = query("""
        SELECT task_type,
               MIN(ran_at AT TIME ZONE 'Europe/Kyiv') as first_run_today,
               bool_or(status = 'ok') as has_ok
        FROM public.etl_log
        WHERE ran_at >= NOW() - INTERVAL '24 hours'
        GROUP BY task_type
    """)
    if not r:
        return {}
    return {row[0]: {"first_run": row[1], "has_ok": row[2]} for row in r}

@st.cache_data(ttl=60)
def load_today_runs():
    r = query("""
        SELECT task_type,
               (ran_at AT TIME ZONE 'Europe/Kyiv')::text as ran_kyiv,
               rows_saved,
               elapsed_sec::int,
               status
        FROM public.etl_log
        WHERE ran_at >= NOW() - INTERVAL '24 hours'
        ORDER BY ran_at DESC
    """)
    if not r:
        return []
    return r

@st.cache_data(ttl=60)
def load_context_for_ai():
    ctx = {}
    etl = load_etl_log()
    ctx["etl_log"] = {k: {
        "ran_at": str(v.get("ran_at",""))[:16],
        "rows": v.get("rows_saved", 0),
        "elapsed_sec": float(v.get("elapsed_sec") or 0),
        "status": v.get("status","")
    } for k, v in etl.items()}
    m = load_system_metrics()
    if m:
        ctx["system"] = {
            "cpu": m.get("cpu"), "ram_pct": m.get("ram_pct"),
            "disk_pct": m.get("disk_pct"), "db_size_mb": m.get("db_size_mb"),
            "run_forever_alive": m.get("alive"),
            "run_forever_uptime_sec": m.get("uptime_sec"),
            "running_workers": m.get("workers"),
        }
    db = load_db_overview()
    if not db.empty:
        top = db.head(10)[["full_name","size_mb","row_count"]].to_dict("records")
        ctx["top_tables"] = top
    now_dt = datetime.now()
    alerts = []
    for task_type, threshold_h in ALERT_THRESHOLDS.items():
        log = etl.get(task_type)
        if not log or not log.get("ran_at"):
            alerts.append({"task": task_type, "issue": "never_ran"})
            continue
        ran_at = log["ran_at"]
        try:
            ran_naive = ran_at.replace(tzinfo=None) if hasattr(ran_at,"tzinfo") and ran_at.tzinfo else ran_at
            h_ago = (now_dt - ran_naive).total_seconds() / 3600
            if h_ago > threshold_h:
                alerts.append({"task": task_type, "hours_ago": round(h_ago,1), "threshold": threshold_h})
        except:
            pass
    ctx["alerts"] = alerts
    return ctx

def fmt_uptime(sec):
    if not sec:
        return "—"
    h = sec // 3600
    m = (sec % 3600) // 60
    if h > 0:
        return f"{h}г {m:02d}хв"
    return f"{m}хв"

def gauge_html(value, max_val, color, label, unit="%"):
    pct = min(100, round((value or 0) / max_val * 100))
    bar_color = "#22c55e" if pct < 60 else "#f59e0b" if pct < 85 else "#ef4444"
    return f"""
    <div style="background:{bg2};border:1px solid {border};border-radius:10px;padding:14px 16px;margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px">
            <span style="font-size:12px;color:{text4};font-weight:600;text-transform:uppercase;letter-spacing:.08em">{label}</span>
            <span style="font-size:20px;font-weight:700;color:{bar_color};font-family:JetBrains Mono,monospace">{value}{unit}</span>
        </div>
        <div style="height:6px;background:{"#1a2235" if dark else "#e2e8f0"};border-radius:3px;overflow:hidden">
            <div style="width:{pct}%;height:100%;background:{bar_color};border-radius:3px;transition:width .3s"></div>
        </div>
    </div>"""

# ============================================================
# SIDEBAR
# ============================================================

def render_sidebar():
    now = now_kyiv()
    with st.sidebar:
        st.markdown(f"""
        <div class="sb-logo">
            <img src="https://udcparts.com/cdn/shop/files/logo.svg?v=1701894617&width=300" alt="UDC">
            <div class="sb-title">ETL Monitor</div>
            <div class="sb-sub">{now.strftime('%H:%M')} Kyiv · v4.2</div>
        </div>
        <hr class="sb-divider">
        """, unsafe_allow_html=True)

        pages = ["📊 Статус", "📈 Аналітика", "🗄️ База даних", "🖥️ Система", "🤖 AI", "🗺️ Архітектура"]
        for p in pages:
            active = st.session_state.page == p
            if st.button(p, use_container_width=True, key=f"nav_{p}", type="primary" if active else "secondary"):
                st.session_state.page = p
                st.rerun()

        st.markdown("<hr class='sb-divider'>", unsafe_allow_html=True)

        theme_label = "☀️ Світла тема" if dark else "🌙 Темна тема"
        if st.button(theme_label, use_container_width=True):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()

        try:
            etl = load_etl_log()
            ok_n = sum(1 for v in etl.values() if v.get("status") == "ok")
            prob_n = sum(1 for v in etl.values() if v.get("status") == "error")
        except:
            ok_n, prob_n = 0, 0
        st.markdown(f"""
        <hr class="sb-divider">
        <div style="padding:8px 16px;font-size:12px;font-family:JetBrains Mono,monospace">
            <div style="color:#22c55e;margin-bottom:4px">✅ OK: {ok_n}</div>
            <div style="color:{'#ef4444' if prob_n > 0 else '#3a4a6b'}">{'⚠️' if prob_n > 0 else '●'} Проблем: {prob_n}</div>
        </div>
        """, unsafe_allow_html=True)

        return st.session_state.page

# ============================================================
# PAGE: STATUS
# ============================================================
def page_status(data):
    now = now_kyiv()
    st.markdown(f"""
    <div class="page-header">
        <div class="page-title">📊 Статус завантажувачів</div>
        <div class="page-sub">Актуальні дані з БД · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
    </div>
    """, unsafe_allow_html=True)

    ok_n    = sum(1 for r in data if r["status"] == "ok")
    prob_n  = sum(1 for r in data if r["status"] in ("warn", "stale"))
    empty_n = sum(1 for r in data if r["status"] == "empty")

    st.markdown(f"""
    <div class="metrics-row">
        <div class="metric m-ok"><div class="metric-num">{ok_n}</div><div class="metric-lbl">✅ OK</div></div>
        <div class="metric m-warn"><div class="metric-num">{prob_n}</div><div class="metric-lbl">⚠️ Проблеми</div></div>
        <div class="metric m-stale"><div class="metric-num">{empty_n}</div><div class="metric-lbl">⬜ Порожні</div></div>
        <div class="metric m-total"><div class="metric-num">{len(data)}</div><div class="metric-lbl">📋 Всього</div></div>
    </div>
    """, unsafe_allow_html=True)

    col_f, col_r = st.columns([5, 1])
    with col_f:
        flt = st.selectbox("f", ["Всі таблиці", "✅ OK", "⚠️ Проблеми", "⬜ Порожні"], label_visibility="collapsed")
    with col_r:
        if st.button("⟳ Refresh", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    filtered = data
    if flt == "✅ OK":
        filtered = [r for r in data if r["status"] == "ok"]
    elif flt == "⚠️ Проблеми":
        filtered = [r for r in data if r["status"] in ("warn", "stale")]
    elif flt == "⬜ Порожні":
        filtered = [r for r in data if r["status"] == "empty"]

    filtered = sorted(filtered, key=lambda x: {"stale": 0, "warn": 1, "empty": 2, "ok": 3}.get(x["status"], 9))

    badge_map = {
        "ok":    '<span class="badge b-ok"><span class="dot dot-ok"></span>OK</span>',
        "warn":  '<span class="badge b-warn"><span class="dot dot-warn"></span>Увага</span>',
        "stale": '<span class="badge b-stale"><span class="dot dot-stale"></span>Застарів</span>',
        "empty": '<span class="badge b-empty"><span class="dot dot-empty"></span>Порожня</span>',
    }
    last_cls = {"ok": "c-ok", "warn": "c-warn", "stale": "c-stale", "empty": "c-empty"}

    rows_html = ""
    for r in filtered:
        cnt = f"{r['count']:,}" if r["count"] is not None else "—"
        rows_html += f"""<tr>
            <td class="c-name">{r['icon']} {r['label']}</td>
            <td class="c-tbl">{r['table']}</td>
            <td class="c-cnt">{cnt}</td>
            <td class="{last_cls.get(r['status'], 'c-empty')}">{r['last_str']}</td>
            <td class="c-ran">{r['ran_at']}</td>
            <td class="c-nxt">{r['next_str']}</td>
            <td><span class="freq-pill">{r['freq']}</span></td>
            <td>{badge_map.get(r['status'], '')}</td>
        </tr>"""

    st.markdown(f"""
    <div class="etl-wrap">
        <table class="etl-table">
            <thead><tr>
                <th>Модуль</th><th>Таблиця</th><th>Рядків</th>
                <th>Останнє</th><th>Запущено</th><th>Наступний</th><th>Частота</th><th>Статус</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>
    <div class="etl-footer">cache 2хв · {len(filtered)}/{len(data)} таблиць · Kyiv TZ</div>
    """, unsafe_allow_html=True)

    # ── Черга pending_reports
    queue = load_pending_queue()
    if queue:
        pending_n  = sum(r[2] for r in queue if r[1] == 'pending')
        download_n = sum(r[2] for r in queue if r[1] == 'downloading')
        error_n    = sum(r[2] for r in queue if r[1] == 'error')
        rows_q = ""
        for task_type, status, cnt, oldest in queue:
            age = ""
            if oldest:
                oldest_naive = oldest.replace(tzinfo=None) if hasattr(oldest,'tzinfo') and oldest.tzinfo else oldest
                mins = int((datetime.now() - oldest_naive).total_seconds() / 60)
                age = f"{mins}хв" if mins < 60 else f"{mins//60}г {mins%60:02d}хв"
            color = {"pending": "#f59e0b", "downloading": "#3b82f6", "error": "#ef4444"}.get(status, text4)
            emoji = {"pending": "⏳", "downloading": "⬇️", "error": "💥"}.get(status, "●")
            rows_q += f"""<tr>
                <td style="padding:8px 14px;color:{text1};font-weight:600">{emoji} {task_type}</td>
                <td style="padding:8px 14px;color:{color};font-weight:600">{status}</td>
                <td style="padding:8px 14px;color:{text2};font-family:JetBrains Mono,monospace">{cnt}</td>
                <td style="padding:8px 14px;color:{text4};font-family:JetBrains Mono,monospace;font-size:11px">{age}</td>
            </tr>"""
        st.markdown(f"""
        <div style="margin-top:16px">
        <div class="etl-wrap">
            <div style="padding:10px 14px;border-bottom:1px solid {border};display:flex;align-items:center;gap:16px">
                <span style="font-size:12px;font-weight:700;color:{text1}">📥 Collector Queue</span>
                <span style="font-size:11px;color:#f59e0b">⏳ {pending_n} pending</span>
                <span style="font-size:11px;color:#3b82f6">⬇️ {download_n} downloading</span>
                {('<span style="font-size:11px;color:#ef4444">💥 ' + str(error_n) + ' error</span>') if error_n else ''}
            </div>
            <table class="etl-table">
                <thead><tr><th>Task</th><th>Статус</th><th>Кількість</th><th>Вік</th></tr></thead>
                <tbody>{rows_q}</tbody>
            </table>
        </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Collector Health
    ch = load_collector_health()
    if ch:
        last_col = ch.get("last_collected")
        if last_col:
            last_naive = last_col.replace(tzinfo=None) if hasattr(last_col, 'tzinfo') and last_col.tzinfo else last_col
            age_sec = (datetime.now() - last_naive).total_seconds()
            age_str = f"{int(age_sec/60)}хв тому" if age_sec < 3600 else f"{int(age_sec/3600)}г тому"
            health_color = "#22c55e" if age_sec < 1800 else "#f59e0b" if age_sec < 3600 else "#ef4444"
            health_text  = "HEALTHY" if age_sec < 1800 else "SLOW" if age_sec < 3600 else "STALE"
        else:
            age_str = "—"
            health_color = "#ef4444"
            health_text  = "NO DATA"

        oldest_pend = ch.get("oldest_pending")
        pend_age_str = ""
        pend_color = text4
        if oldest_pend:
            op_naive = oldest_pend.replace(tzinfo=None) if hasattr(oldest_pend, 'tzinfo') and oldest_pend.tzinfo else oldest_pend
            pend_min = int((datetime.now() - op_naive).total_seconds() / 60)
            pend_age_str = f"{pend_min}хв" if pend_min < 60 else f"{pend_min//60}г {pend_min%60:02d}хв"
            pend_color = "#22c55e" if pend_min < 30 else "#f59e0b" if pend_min < 60 else "#ef4444"

        st.markdown(f"""
        <div style="margin-top:16px">
        <div class="etl-wrap">
            <div style="padding:12px 14px;display:flex;align-items:center;gap:20px;flex-wrap:wrap">
                <span style="font-size:12px;font-weight:700;color:{text1}">📥 Collector Health</span>
                <span style="display:inline-flex;align-items:center;gap:6px;font-size:11px;font-weight:700;color:{health_color}">
                    <span style="width:8px;height:8px;border-radius:50%;background:{health_color};display:inline-block"></span>
                    {health_text}
                </span>
                <span style="font-size:11px;color:{text4}">⏱️ Останній збір: <span style="color:{health_color}">{age_str}</span></span>
                <span style="font-size:11px;color:#22c55e">✅ Сьогодні: {ch['done_today']} звітів</span>
                <span style="font-size:11px;color:#3b82f6">📊 Всього: {ch['total_done']}</span>
                {f'<span style="font-size:11px;color:#f59e0b">⏳ Pending: {ch["pending_now"]} (вік: <span style="color:{pend_color}">{pend_age_str}</span>)</span>' if ch['pending_now'] > 0 else ''}
                {f'<span style="font-size:11px;color:#ef4444">💥 Errors: {ch["errors"]}</span>' if ch['errors'] > 0 else ''}
            </div>
        </div>
        </div>
        """, unsafe_allow_html=True)

    # ── SLA Monitor
    sla = load_sla_status()
    now_kyiv_dt = now_kyiv()
    sla_rows = ""
    sla_ok = sla_miss = sla_pending = 0

    for task, deadline_str in SLA_DEADLINES.items():
        dh, dm = map(int, deadline_str.split(":"))
        deadline_dt = KYIV_TZ.localize(datetime(now_kyiv_dt.year, now_kyiv_dt.month, now_kyiv_dt.day, dh, dm))
        past_deadline = now_kyiv_dt > deadline_dt

        log = sla.get(task)
        if log and log.get("has_ok"):
            first_run = log["first_run"]
            if first_run:
                fr_naive = first_run.replace(tzinfo=None) if hasattr(first_run, 'tzinfo') and first_run.tzinfo else first_run
                fr_str = fr_naive.strftime("%H:%M:%S")
                # Перевіряємо чи виконався до дедлайну
                fr_kyiv = KYIV_TZ.localize(fr_naive) if not (hasattr(first_run, 'tzinfo') and first_run.tzinfo) else first_run
                on_time = fr_kyiv <= deadline_dt
                status_icon = '✅' if on_time else '⚠️'
                status_color = "#22c55e" if on_time else "#f59e0b"
                status_label = "вчасно" if on_time else "запізнення"
                sla_ok += 1
            else:
                fr_str = "—"
                status_icon = "❓"
                status_color = text4
                status_label = "—"
        elif past_deadline:
            fr_str = "—"
            status_icon = "🔴"
            status_color = "#ef4444"
            status_label = "ПРОПУЩЕНО"
            sla_miss += 1
        else:
            fr_str = "—"
            status_icon = "⏳"
            status_color = text4
            status_label = f"до {deadline_str}"
            sla_pending += 1

        sla_rows += f"""<tr>
            <td style="padding:7px 14px;color:{text1};font-weight:600;font-family:JetBrains Mono,monospace;font-size:12px">{task}</td>
            <td style="padding:7px 14px;color:{text4};font-family:JetBrains Mono,monospace;font-size:11px">{deadline_str}</td>
            <td style="padding:7px 14px;color:#4a9e6b;font-family:JetBrains Mono,monospace;font-size:12px">{fr_str}</td>
            <td style="padding:7px 14px;color:{status_color};font-weight:600;font-size:12px">{status_icon} {status_label}</td>
        </tr>"""

    st.markdown(f"""
    <div style="margin-top:16px">
    <div class="etl-wrap">
        <div style="padding:10px 14px;border-bottom:1px solid {border};display:flex;align-items:center;gap:16px;flex-wrap:wrap">
            <span style="font-size:12px;font-weight:700;color:{text1}">🎯 SLA Monitor</span>
            <span style="font-size:11px;color:#22c55e">✅ Вчасно: {sla_ok}</span>
            {f'<span style="font-size:11px;color:#ef4444">🔴 Пропущено: {sla_miss}</span>' if sla_miss > 0 else ''}
            <span style="font-size:11px;color:{text4}">⏳ Очікується: {sla_pending}</span>
        </div>
        <table class="etl-table">
            <thead><tr>
                <th>Task</th><th>Дедлайн</th><th>Виконано о</th><th>SLA</th>
            </tr></thead>
            <tbody>{sla_rows}</tbody>
        </table>
    </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Сьогодні (24г)
    today_runs = load_today_runs()
    if today_runs:
        rows_t = ""
        for row in today_runs:
            task_type, ran_at, rows_n, elapsed, status = row
            t = str(ran_at)[11:19] if ran_at else "—"
            r = f"{rows_n:,}" if rows_n else "—"
            if elapsed and elapsed > 60:
                e = f"{elapsed//60}хв {elapsed%60}с"
            elif elapsed:
                e = f"{elapsed}с"
            else:
                e = "—"
            s = f'<span style="color:#22c55e;font-weight:700">✅</span>' if status == "ok" else f'<span style="color:#ef4444;font-weight:700">❌</span>'
            rows_t += f"""<tr>
                <td style="padding:8px 14px;color:{text1};font-weight:600;font-family:JetBrains Mono,monospace;font-size:12px">{task_type}</td>
                <td style="padding:8px 14px;color:#4a9e6b;font-family:JetBrains Mono,monospace;font-size:12px">{t}</td>
                <td style="padding:8px 14px;color:{text2};font-family:JetBrains Mono,monospace;font-size:12px">{r}</td>
                <td style="padding:8px 14px;color:{text4};font-family:JetBrains Mono,monospace;font-size:12px">{e}</td>
                <td style="padding:8px 14px">{s}</td>
            </tr>"""

        total_ok  = sum(1 for *_, s in today_runs if s == "ok")
        total_err = sum(1 for *_, s in today_runs if s != "ok")
        total_rows_today = sum(r[2] or 0 for r in today_runs)

        st.markdown(f"""
        <div style="margin-top:16px">
        <div class="etl-wrap">
            <div style="padding:10px 14px;border-bottom:1px solid {border};display:flex;align-items:center;gap:16px;flex-wrap:wrap">
                <span style="font-size:12px;font-weight:700;color:{text1}">📅 Сьогодні (24г)</span>
                <span style="font-size:11px;color:#22c55e">✅ {total_ok} ok</span>
                {(f'<span style="font-size:11px;color:#ef4444">❌ {total_err} помилок</span>') if total_err else ''}
                <span style="font-size:11px;color:{text4}">{len(today_runs)} запусків</span>
                <span style="font-size:11px;color:#3b82f6">💾 {total_rows_today:,} рядків</span>
            </div>
            <table class="etl-table">
                <thead><tr>
                    <th>Task</th><th>Час (Kyiv)</th><th>Рядків</th><th>Тривалість</th><th>Статус</th>
                </tr></thead>
                <tbody>{rows_t}</tbody>
            </table>
        </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Бар чарт — рядків по task за сьогодні
    if today_runs:
        task_rows = {}
        for row in today_runs:
            task_type, _, rows_n, _, status = row
            if rows_n and status == "ok":
                task_rows[task_type] = task_rows.get(task_type, 0) + (rows_n or 0)

        task_rows = {k: v for k, v in task_rows.items() if v > 0}
        if task_rows:
            sorted_tasks = sorted(task_rows.items(), key=lambda x: x[1], reverse=True)
            max_v = max(v for _, v in sorted_tasks) or 1
            n = len(sorted_tasks)
            w, h = 900, 180
            pad_l = 140
            bar_h = max(10, (h - n * 4) // n)

            bars = ""
            for i, (task, val) in enumerate(sorted_tasks):
                bar_w = max(2, int(val / max_v * (w - pad_l - 20)))
                y = i * (bar_h + 4)
                label = f"{val/1000:.0f}K" if val >= 1000 else str(val)
                bars += f'<rect x="{pad_l}" y="{y}" width="{bar_w}" height="{bar_h}" fill="#3b82f6" opacity=".8" rx="3"/>'
                bars += f'<text x="{pad_l - 6}" y="{y + bar_h//2 + 4}" text-anchor="end" font-size="11" fill="{text2}" font-family="JetBrains Mono,monospace">{task}</text>'
                bars += f'<text x="{pad_l + bar_w + 6}" y="{y + bar_h//2 + 4}" font-size="11" fill="{text1}" font-family="JetBrains Mono,monospace">{label}</text>'

            chart_h = n * (bar_h + 4)
            svg = f'<svg viewBox="0 0 {w} {chart_h + 10}" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-height:350px">{bars}</svg>'

            st.markdown(f"""
            <div style="margin-top:12px">
            <div class="stat-card"><h4>📊 Рядків збережено сьогодні по воркерам</h4>
            {svg}
            </div>
            </div>
            """, unsafe_allow_html=True)

# ============================================================
# PAGE: ANALYTICS
# ============================================================

def page_analytics():
    now = now_kyiv()
    st.markdown(f"""
    <div class="page-header">
        <div class="page-title">📈 Аналітика</div>
        <div class="page-sub">Останні 30 днів · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
    </div>
    """, unsafe_allow_html=True)

    df = load_etl_log_history(30)
    etl_log = load_etl_log()

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown('<div class="stat-card"><h4>🟢 Uptime воркерів (% успішних запусків)</h4>', unsafe_allow_html=True)
        if not df.empty:
            uptime = df.groupby("task_type").apply(
                lambda x: round(100 * (x["status"] == "ok").sum() / len(x), 1)
            ).sort_values(ascending=True)
            bars_html = ""
            for task, pct in uptime.items():
                color = "#22c55e" if pct >= 95 else "#f59e0b" if pct >= 80 else "#ef4444"
                count = len(df[df["task_type"] == task])
                bars_html += f"""
                <div class="uptime-row">
                    <div class="uptime-label">{task}</div>
                    <div class="uptime-bar-wrap">
                        <div style="width:{pct}%;height:100%;background:{color};border-radius:4px;transition:width .3s"></div>
                    </div>
                    <div class="uptime-pct" style="color:{color}">{pct}%</div>
                    <div style="font-size:10px;color:{text4};font-family:JetBrains Mono,monospace;min-width:50px">{count} runs</div>
                </div>"""
            st.markdown(bars_html, unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="color:{text4};font-size:13px">Немає даних в etl_log</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="stat-card"><h4>🔴 Алерти — не запускались більше порогу</h4>', unsafe_allow_html=True)
        alerts = []
        now_dt = datetime.now()
        for task_type, threshold_h in ALERT_THRESHOLDS.items():
            log = etl_log.get(task_type)
            if not log or not log.get("ran_at"):
                alerts.append((task_type, None, threshold_h))
                continue
            ran_at = log["ran_at"]
            ran_naive = ran_at.replace(tzinfo=None) if hasattr(ran_at, 'tzinfo') and ran_at.tzinfo else ran_at
            h_ago = (now_dt - ran_naive).total_seconds() / 3600
            if h_ago > threshold_h:
                alerts.append((task_type, h_ago, threshold_h))

        if alerts:
            alerts_html = ""
            for task_type, h_ago, threshold_h in alerts:
                info = f"{h_ago:.0f}г без запуску (поріг: {threshold_h}г)" if h_ago else "ніколи не запускався"
                alerts_html += f"""
                <div class="alert-row">
                    <div>
                        <div class="alert-name">⚠️ {task_type}</div>
                        <div class="alert-info">{info}</div>
                    </div>
                </div>"""
            st.markdown(alerts_html, unsafe_allow_html=True)
            if st.button("📱 Надіслати в Telegram", use_container_width=True):
                lines = ["🔴 <b>ETL Monitor — Алерти</b>\n"]
                for task_type, h_ago, threshold_h in alerts:
                    info = f"{h_ago:.0f}г без запуску" if h_ago else "ніколи не запускався"
                    lines.append(f"⚠️ {task_type}: {info}")
                sent = _send_tg_alert("\n".join(lines))
                if sent:
                    st.success(f"✅ Надіслано {sent} підписникам!")
                else:
                    st.warning("⚠️ Не вдалось — перевір TELEGRAM_BOT_TOKEN і підписників")
        else:
            st.markdown(f'<div style="color:#22c55e;font-size:14px;padding:8px 0">✅ Всі воркери в нормі</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Теплова карта
    st.markdown('<div class="stat-card"><h4>🗓️ Теплова карта запусків (останні 30 днів)</h4>', unsafe_allow_html=True)
    if not df.empty:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(29, -1, -1)]
        all_tasks = sorted(df["task_type"].unique())
        pivot_dict = {}
        for _, row in df.groupby(["task_type", "date"]).size().reset_index(name="runs").iterrows():
            pivot_dict[(row["task_type"], row["date"])] = row["runs"]

        def cell_color(runs):
            if runs == 0: return "#1a2235" if dark else "#ebedf0"
            elif runs == 1: return "#1a5c3a"
            elif runs <= 3: return "#26a148"
            elif runs <= 5: return "#2ecc71"
            else: return "#57e89c"

        date_headers = "<th></th>"
        for i, d in enumerate(dates):
            label = d.strftime("%d.%m") if i % 7 == 0 else ""
            date_headers += f'<th class="hm-date-label">{label}</th>'

        table_rows = f"<tr>{date_headers}</tr>"
        for task in all_tasks:
            cells = f'<td class="hm-label">{task}</td>'
            for d in dates:
                runs = pivot_dict.get((task, d), 0)
                color = cell_color(runs)
                cells += f'<td class="cell" title="{runs} runs · {d}" style="background:{color};width:16px;height:16px;border-radius:3px"></td>'
            table_rows += f"<tr>{cells}</tr>"

        legend = f"""
        <div style="display:flex;align-items:center;gap:6px;margin-top:12px;font-size:11px;color:{text4}">
            <span>Менше</span>
            {"".join(f'<div style="width:14px;height:14px;background:{c};border-radius:2px"></div>' for c in ["#1a2235" if dark else "#ebedf0", "#1a5c3a", "#26a148", "#2ecc71", "#57e89c"])}
            <span>Більше</span>
        </div>"""
        st.markdown(f'<div class="hm-wrap"><table class="hm-table">{table_rows}</table>{legend}</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="color:{text4};font-size:13px;padding:8px 0">Дані з\'являться після першого запуску воркерів.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # CPU/RAM
    st.markdown('<div class="stat-card"><h4>🖥️ CPU / RAM по часу (останні 2 години)</h4>', unsafe_allow_html=True)
    cpu_df = load_cpu_history()
    if not cpu_df.empty:
        cpu_df["time"] = pd.to_datetime(cpu_df["collected_at"], utc=True).dt.tz_convert(KYIV_TZ).dt.strftime("%H:%M")
        cpu_df = cpu_df.dropna(subset=["cpu_pct", "ram_pct"])
        cpu_df["cpu_pct"] = pd.to_numeric(cpu_df["cpu_pct"], errors="coerce").fillna(0)
        cpu_df["ram_pct"] = pd.to_numeric(cpu_df["ram_pct"], errors="coerce").fillna(0)
        cpu_vals = cpu_df["cpu_pct"].tolist()
        ram_vals = cpu_df["ram_pct"].tolist()
        times = cpu_df["time"].tolist()
        n = len(times)
        if n > 1:
            w, h = 900, 140
            pad = 40
            def scale_x(i): return pad + i * (w - pad*2) / max(n-1,1)
            def scale_y(v): return h - pad//2 - (v / 100) * (h - pad)
            cpu_points = " ".join(f"{scale_x(i):.1f},{scale_y(v):.1f}" for i,v in enumerate(cpu_vals))
            ram_points = " ".join(f"{scale_x(i):.1f},{scale_y(v):.1f}" for i,v in enumerate(ram_vals))
            x_labels = ""
            step = max(1, n//6)
            for i in range(0, n, step):
                x_labels += f'<text x="{scale_x(i):.1f}" y="{h+2}" text-anchor="middle" font-size="10" fill="{text4}">{times[i]}</text>'
            y_labels = ""
            for v in [0,25,50,75,100]:
                y = scale_y(v)
                y_labels += f'<text x="{pad-4}" y="{y:.1f}" text-anchor="end" font-size="10" fill="{text4}">{v}%</text>'
                y_labels += f'<line x1="{pad}" y1="{y:.1f}" x2="{w-pad}" y2="{y:.1f}" stroke="{border}" stroke-width="1" stroke-dasharray="3,3"/>'
            svg = f"""<svg viewBox="0 0 {w} {h+14}" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-height:160px">
                {y_labels}
                <polyline points="{ram_points}" fill="none" stroke="#8b5cf6" stroke-width="2" stroke-linejoin="round"/>
                <polyline points="{cpu_points}" fill="none" stroke="#3b82f6" stroke-width="2" stroke-linejoin="round"/>
                {x_labels}
                <circle cx="{scale_x(n-1):.1f}" cy="{scale_y(cpu_vals[-1]):.1f}" r="4" fill="#3b82f6"/>
                <circle cx="{scale_x(n-1):.1f}" cy="{scale_y(ram_vals[-1]):.1f}" r="4" fill="#8b5cf6"/>
                <text x="{scale_x(n-1)+6:.1f}" y="{scale_y(cpu_vals[-1]):.1f}" font-size="11" fill="#3b82f6">CPU {cpu_vals[-1]:.1f}%</text>
                <text x="{scale_x(n-1)+6:.1f}" y="{scale_y(ram_vals[-1])+12:.1f}" font-size="11" fill="#8b5cf6">RAM {ram_vals[-1]:.1f}%</text>
            </svg>"""
            st.markdown(svg, unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:11px;color:{text4};margin-top:4px">🔵 CPU &nbsp;&nbsp; 🟣 RAM</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="color:{text4};font-size:13px">Дані зʼявляться після запуску system_monitor.py</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    col3, col4 = st.columns([1, 1])

    with col3:
        st.markdown('<div class="stat-card"><h4>⏱️ Час виконання воркерів (хв)</h4>', unsafe_allow_html=True)
        if not df.empty:
            avg_times = df[df["elapsed_min"] > 0].groupby("task_type")["elapsed_min"].agg(["mean","max"]).round(2)
            avg_times = avg_times.sort_values("mean", ascending=False).head(12)
            if not avg_times.empty:
                max_val = avg_times["mean"].max()
                bars_html = ""
                for task, row in avg_times.iterrows():
                    bar_w = max(2, int(row["mean"] / max_val * 180))
                    color = "#22c55e" if row["mean"] < 1 else "#f59e0b" if row["mean"] < 5 else "#ef4444"
                    bars_html += f"""<div style="display:flex;align-items:center;gap:8px;margin-bottom:7px">
                        <div style="min-width:120px;color:{text2};font-size:11px;font-family:JetBrains Mono,monospace;text-align:right">{task}</div>
                        <div style="width:{bar_w}px;height:8px;background:{color};border-radius:4px;opacity:.85"></div>
                        <div style="color:{text1};font-size:11px;font-family:JetBrains Mono,monospace">{row["mean"]:.2f}хв</div>
                        <div style="color:{text4};font-size:10px;font-family:JetBrains Mono,monospace">max {row["max"]:.1f}</div>
                    </div>"""
                st.markdown(bars_html, unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="color:{text4};font-size:13px">Немає даних</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="stat-card"><h4>📈 Рядків збережено по днях</h4>', unsafe_allow_html=True)
        if not df.empty:
            daily = df[df["rows_saved"] > 0].groupby("date")["rows_saved"].sum().reset_index()
            daily = daily.sort_values("date").tail(14)
            if not daily.empty:
                dates = daily["date"].astype(str).tolist()
                vals = daily["rows_saved"].tolist()
                max_v = max(vals) or 1
                n = len(dates)
                w2, h2 = 400, 120
                pad2 = 35
                bar_w2 = max(4, (w2 - pad2*2) // max(n,1) - 3)
                bars = ""
                for i, (d, v) in enumerate(zip(dates, vals)):
                    bh = max(2, int(v / max_v * (h2 - pad2)))
                    x = pad2 + i * ((w2 - pad2*2) // max(n,1))
                    y = h2 - pad2 - bh
                    bars += f'<rect x="{x}" y="{y}" width="{bar_w2}" height="{bh}" fill="#3b82f6" opacity=".8" rx="2"/>'
                    if i % 3 == 0:
                        bars += f'<text x="{x+bar_w2//2}" y="{h2-2}" text-anchor="middle" font-size="9" fill="{text4}">{d[5:]}</text>'
                svg2 = f'<svg viewBox="0 0 {w2} {h2+4}" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-height:140px">{bars}</svg>'
                st.markdown(svg2, unsafe_allow_html=True)
                total_rows = sum(vals)
                st.markdown(f'<div style="font-size:11px;color:{text4};margin-top:4px">Всього за 14 днів: {total_rows:,} рядків</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="color:{text4};font-size:13px">Немає даних</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown(f'<div class="etl-footer">cache 2хв · 30 днів · Kyiv TZ</div>', unsafe_allow_html=True)


def _send_tg_alert(text):
    import requests as req
    bot_token = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
    if not bot_token:
        return 0
    subs = []
    chat_ids_str = st.secrets.get("TELEGRAM_CHAT_IDS", "")
    if chat_ids_str:
        subs = [c.strip() for c in chat_ids_str.split(",") if c.strip()]
    if not subs:
        try:
            db_url = st.secrets.get("DATABASE_URL", "")
            conn = psycopg2.connect(db_url.replace("postgres://", "postgresql://", 1), sslmode="require", connect_timeout=5)
            cur = conn.cursor()
            try:
                cur.execute("SELECT chat_id FROM public.telegram_subscribers WHERE subscribed = TRUE LIMIT 20")
                subs = [str(r[0]) for r in cur.fetchall()]
            except:
                conn.rollback()
            cur.close()
            conn.close()
        except:
            pass
    if not subs:
        try:
            db_url = st.secrets.get("DATABASE_URL", "")
            conn = psycopg2.connect(db_url.replace("postgres://", "postgresql://", 1), sslmode="require", connect_timeout=5)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT chat_id FROM public.etl_tg_subscribers WHERE active = TRUE LIMIT 20")
            subs = [str(r[0]) for r in cur.fetchall()]
            cur.close()
            conn.close()
        except:
            pass
    if not subs:
        return 0
    sent = 0
    for cid in subs:
        try:
            r = req.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "HTML"}, timeout=10)
            if r.status_code == 200:
                sent += 1
        except:
            pass
    return sent


def page_system():
    now = now_kyiv()
    st.markdown(f"""
    <div class="page-header">
        <div class="page-title">🖥️ Система</div>
        <div class="page-sub">Сервер · БД · Процеси · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
    </div>
    """, unsafe_allow_html=True)

    m = load_system_metrics()
    if not m:
        st.warning("⚠️ Немає даних — запусти `python system_monitor.py` на сервері")
        return

    collected = m.get("collected_at")
    if collected:
        collected_naive = collected.replace(tzinfo=None) if hasattr(collected, "tzinfo") and collected.tzinfo else collected
        age_sec = (datetime.now() - collected_naive).total_seconds()
        age_str = f"{int(age_sec)}с тому" if age_sec < 120 else f"{int(age_sec/60)}хв тому"
        freshness_color = "#22c55e" if age_sec < 60 else "#f59e0b" if age_sec < 180 else "#ef4444"
        st.markdown(f'<div style="font-size:11px;color:{freshness_color};font-family:JetBrains Mono,monospace;margin-bottom:12px">● Дані оновлено {age_str}</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        st.markdown(f'<div class="stat-card"><h4>🖥️ Сервер</h4>', unsafe_allow_html=True)
        st.markdown(gauge_html(m.get("cpu"), 100, "#3b82f6", "CPU"), unsafe_allow_html=True)
        st.markdown(gauge_html(m.get("ram_pct"), 100, "#8b5cf6", "RAM",
            unit=f"% ({m.get('ram_used_mb',0)//1024}GB / {m.get('ram_total_mb',0)//1024}GB)"), unsafe_allow_html=True)
        st.markdown(gauge_html(m.get("disk_pct"), 100, "#06b6d4", "Disk",
            unit=f"% ({m.get('disk_used_gb',0):.0f} / {m.get('disk_total_gb',0):.0f} GB)"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown(f'<div class="stat-card"><h4>⚙️ Процеси</h4>', unsafe_allow_html=True)
        alive = m.get("alive", False)
        pid = m.get("pid")
        uptime = fmt_uptime(m.get("uptime_sec"))
        workers = m.get("workers") or "—"
        status_color = "#22c55e" if alive else "#ef4444"
        status_text = "RUNNING" if alive else "STOPPED"
        st.markdown(f"""
        <div style="background:{"rgba(34,197,94,.08)" if alive else "rgba(239,68,68,.08)"};border:1px solid {"rgba(34,197,94,.2)" if alive else "rgba(239,68,68,.2)"};border-radius:10px;padding:14px 16px;margin-bottom:10px">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
                <div style="width:8px;height:8px;border-radius:50%;background:{status_color}"></div>
                <span style="font-size:14px;font-weight:700;color:{status_color}">run_forever.py</span>
                <span style="font-size:11px;color:{status_color};padding:1px 8px;border-radius:10px">{status_text}</span>
            </div>
            <div style="font-size:12px;color:{text2};font-family:JetBrains Mono,monospace">PID: {pid or "—"} · Uptime: {uptime}</div>
        </div>
        <div style="font-size:11px;color:{text4};margin-bottom:6px;text-transform:uppercase;letter-spacing:.08em;font-weight:600">Запущені воркери:</div>
        <div style="font-size:12px;color:{"#4a9e6b" if workers != "—" else text4};font-family:JetBrains Mono,monospace">{workers}</div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col3:
        st.markdown(f'<div class="stat-card"><h4>🗄️ База даних</h4>', unsafe_allow_html=True)
        db_size = m.get("db_size_mb", 0) or 0
        db_conn = m.get("db_conn", 0) or 0
        st.markdown(f"""
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
            <div style="background:{bg3};border:1px solid {border};border-radius:8px;padding:12px;text-align:center">
                <div style="font-size:22px;font-weight:700;color:#3b82f6;font-family:JetBrains Mono,monospace">{db_size:.0f}</div>
                <div style="font-size:10px;color:{text4};text-transform:uppercase;letter-spacing:.08em;margin-top:2px">MB розмір</div>
            </div>
            <div style="background:{bg3};border:1px solid {border};border-radius:8px;padding:12px;text-align:center">
                <div style="font-size:22px;font-weight:700;color:#8b5cf6;font-family:JetBrains Mono,monospace">{db_conn}</div>
                <div style="font-size:10px;color:{text4};text-transform:uppercase;letter-spacing:.08em;margin-top:2px">З'єднань</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown(f'<div class="stat-card"><h4>📦 Розміри таблиць (топ 20)</h4>', unsafe_allow_html=True)
    table_sizes = load_table_sizes()
    if table_sizes:
        max_size = max(r[1] or 0 for r in table_sizes[:20])
        rows_html = ""
        for table_name, size_mb, row_count in table_sizes[:20]:
            bar_w = int((size_mb or 0) / max(max_size, 1) * 200)
            bar_color = "#3b82f6" if (size_mb or 0) < 100 else "#8b5cf6" if (size_mb or 0) < 500 else "#ef4444"
            rows_html += f"""<tr>
                <td style="color:{text2};font-size:12px;padding:7px 12px;font-family:JetBrains Mono,monospace">{table_name}</td>
                <td style="padding:7px 12px">
                    <div style="display:flex;align-items:center;gap:8px">
                        <div style="width:{bar_w}px;height:6px;background:{bar_color};border-radius:3px;opacity:.8;min-width:2px"></div>
                        <span style="color:{text1};font-size:12px;font-family:JetBrains Mono,monospace">{size_mb:.1f} MB</span>
                    </div>
                </td>
                <td style="color:{text4};font-size:11px;padding:7px 12px;font-family:JetBrains Mono,monospace">{int(row_count or 0):,} рядків</td>
            </tr>"""
        st.markdown(f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="color:{text4};font-size:13px">Дані зʼявляться через 5 хвилин після запуску system_monitor.py</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="etl-footer">оновлення 30с · system_monitor.py · Kyiv TZ</div>', unsafe_allow_html=True)


def page_database():
    now = now_kyiv()
    st.markdown(f"""
    <div class="page-header">
        <div class="page-title">🗄️ База даних</div>
        <div class="page-sub">Таблиці · Розміри · Ріст · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
    </div>
    """, unsafe_allow_html=True)

    df = load_db_overview()
    growth = load_growth_per_day()

    if df.empty:
        st.warning("Немає даних")
        return

    total_size = df["size_mb"].sum()
    total_rows = df["row_count"].sum()
    total_tables = len(df)
    schemas = df["schema"].nunique()

    st.markdown(f"""
    <div class="metrics-row">
        <div class="metric m-total"><div class="metric-num">{total_size/1024:.1f}</div><div class="metric-lbl">GB розмір БД</div></div>
        <div class="metric m-ok"><div class="metric-num">{total_rows/1_000_000:.1f}M</div><div class="metric-lbl">Всього рядків</div></div>
        <div class="metric m-warn"><div class="metric-num">{total_tables}</div><div class="metric-lbl">Таблиць</div></div>
        <div class="metric m-stale"><div class="metric-num">{schemas}</div><div class="metric-lbl">Схем</div></div>
    </div>
    """, unsafe_allow_html=True)

    col_s, col_f, col_r = st.columns([2, 3, 1])
    with col_s:
        all_schemas = ["Всі"] + sorted(df["schema"].unique().tolist())
        schema_filter = st.selectbox("Схема", all_schemas, label_visibility="collapsed")
    with col_f:
        search = st.text_input("Пошук", placeholder="Назва таблиці...", label_visibility="collapsed")
    with col_r:
        if st.button("⟳ Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    filtered = df.copy()
    if schema_filter != "Всі":
        filtered = filtered[filtered["schema"] == schema_filter]
    if search:
        filtered = filtered[filtered["table"].str.contains(search, case=False, na=False)]

    max_size = filtered["size_mb"].max() or 1
    schema_colors = {
        "csv": "#3b82f6", "spapi": "#8b5cf6", "api_ad": "#f59e0b",
        "amzudc": "#22c55e", "public": "#6b7280", "upload": "#06b6d4",
        "products": "#ec4899", "finance": "#10b981", "pbi": "#f97316",
        "brand_analytics": "#f43f5e",
    }

    rows_html = ""
    for _, row in filtered.iterrows():
        schema = row["schema"]
        table  = row["table"]
        size   = row["size_mb"]
        rows   = row["row_count"]
        last   = row["last_activity"]
        bar_w = max(2, int(size / max_size * 160))
        bar_color = "#22c55e" if size < 50 else "#f59e0b" if size < 200 else "#ef4444"
        sc_color = schema_colors.get(schema, "#6b7280")
        rows_str = f"{rows/1_000_000:.1f}M" if rows >= 1_000_000 else f"{rows/1000:.0f}K" if rows >= 1000 else str(rows)
        growth_str = "—"
        for task_type, rpd in growth.items():
            for t, sch_tbl, _, _, _ in TASK_MAP:
                if t == task_type and sch_tbl == f"{schema}.{table}":
                    growth_str = f"+{rpd:,}/д" if rpd > 0 else "—"
                    break
        last_str = "—"
        last_color = text4
        try:
            if last:
                last_naive = last.replace(tzinfo=None) if hasattr(last, "tzinfo") and last.tzinfo else last
                h_ago = (datetime.now() - last_naive).total_seconds() / 3600
                if h_ago >= 0:
                    last_str = f"{int(h_ago)}г тому" if h_ago < 48 else f"{int(h_ago/24)}д тому"
                    last_color = "#22c55e" if h_ago < 25 else "#f59e0b" if h_ago < 72 else text4
        except:
            pass

        rows_html += f"""<tr>
            <td style="padding:10px 12px">
                <span style="background:{sc_color}22;color:{sc_color};border:1px solid {sc_color}44;
                    font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;font-family:JetBrains Mono,monospace">{schema}</span>
            </td>
            <td style="padding:10px 12px;color:{text1};font-weight:500;font-size:13px;font-family:JetBrains Mono,monospace">{table}</td>
            <td style="padding:10px 12px">
                <div style="display:flex;align-items:center;gap:8px">
                    <div style="width:{bar_w}px;height:6px;background:{bar_color};border-radius:3px;opacity:.85;min-width:2px"></div>
                    <span style="color:{text1};font-size:12px;font-family:JetBrains Mono,monospace">{size:.1f} MB</span>
                </div>
            </td>
            <td style="padding:10px 12px;color:{text2};font-size:12px;font-family:JetBrains Mono,monospace">{rows_str}</td>
            <td style="padding:10px 12px;color:#4a9e6b;font-size:12px;font-family:JetBrains Mono,monospace">{growth_str}</td>
            <td style="padding:10px 12px;color:{last_color};font-size:11px;font-family:JetBrains Mono,monospace">{last_str}</td>
        </tr>"""

    st.markdown(f"""
    <div class="etl-wrap">
        <table class="etl-table">
            <thead><tr><th>Схема</th><th>Таблиця</th><th>Розмір</th><th>Рядків</th><th>Ріст/день</th><th>Оновлення</th></tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>
    <div class="etl-footer">cache 5хв · {len(filtered)}/{len(df)} таблиць · Kyiv TZ</div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f'<div class="stat-card"><h4>📊 Розбивка по схемах</h4>', unsafe_allow_html=True)
    schema_stats = df.groupby("schema").agg(
        tables=("table","count"), size_mb=("size_mb","sum"), rows=("row_count","sum")
    ).sort_values("size_mb", ascending=False).reset_index()
    max_schema_size = schema_stats["size_mb"].max() or 1
    schema_html = ""
    for _, row in schema_stats.iterrows():
        sc = row["schema"]
        sc_color = schema_colors.get(sc, "#6b7280")
        bar_w = max(2, int(row["size_mb"] / max_schema_size * 200))
        rows_k = f"{row['rows']/1_000_000:.1f}M" if row["rows"] >= 1_000_000 else f"{row['rows']/1000:.0f}K"
        schema_html += f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
            <div style="min-width:90px">
                <span style="background:{sc_color}22;color:{sc_color};border:1px solid {sc_color}44;
                    font-size:11px;font-weight:700;padding:3px 10px;border-radius:4px;font-family:JetBrains Mono,monospace">{sc}</span>
            </div>
            <div style="flex:1;height:8px;background:{"#1a2235" if dark else "#e2e8f0"};border-radius:4px;overflow:hidden">
                <div style="width:{bar_w}px;height:100%;background:{sc_color};border-radius:4px;opacity:.8"></div>
            </div>
            <div style="min-width:70px;text-align:right;color:{text1};font-size:12px;font-family:JetBrains Mono,monospace">{row["size_mb"]:.1f} MB</div>
            <div style="min-width:60px;color:{text4};font-size:11px;font-family:JetBrains Mono,monospace">{rows_k} рядків</div>
            <div style="min-width:50px;color:{text4};font-size:11px;font-family:JetBrains Mono,monospace">{int(row["tables"])} табл</div>
        </div>"""
    st.markdown(schema_html, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def page_ai():
    now = now_kyiv()
    st.markdown(f"""
    <div class="page-header">
        <div class="page-title">🤖 AI Аналітик</div>
        <div class="page-sub">Gemini Flash · запитай про систему · {now.strftime('%H:%M')} Kyiv</div>
    </div>
    """, unsafe_allow_html=True)

    if "ai_messages" not in st.session_state:
        st.session_state.ai_messages = []

    gemini_key   = st.secrets.get("GEMINI_API_KEY", "")
    gemini_model = st.secrets.get("GEMINI_MODEL", "gemini-1.5-flash-latest")

    if not gemini_key:
        st.warning("⚠️ GEMINI_API_KEY не знайдено в secrets")
        return

    ctx = load_context_for_ai()
    import json
    ctx_str = json.dumps(ctx, ensure_ascii=False, default=str, indent=2)

    SYSTEM_PROMPT = f"""Ти — AI аналітик ETL системи Amazon Data Pipeline компанії UDC Parts.
Відповідай коротко, по суті, українською або російською (як запитують).
Використовуй емодзі для наочності.

Поточний стан системи (JSON):
{ctx_str}

Що ти знаєш:
- run_forever.py — планувальник що запускає воркери по розкладу (Київ TZ)
- etl_log — таблиця з часом запуску кожного воркера
- Таблиці: csv.*, spapi.*, api_ad.*, amzudc.* — дані Amazon
- Дашборд на Streamlit Cloud показує статус системи в реальному часі

Якщо питають про проблеми — дивись на alerts і etl_log.
Якщо питають про розміри — дивись top_tables.
Якщо питають про сервер — дивись system."""

    st.markdown(f'<div style="margin-bottom:12px">', unsafe_allow_html=True)
    quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)
    quick_questions = ["Який стан системи зараз?", "Які проблеми є?", "Яка таблиця найбільша?", "Які воркери найповільніші?"]
    for col, q in zip([quick_col1, quick_col2, quick_col3, quick_col4], quick_questions):
        with col:
            if st.button(q, use_container_width=True, key=f"quick_{q[:10]}"):
                st.session_state["ai_pending"] = q
    st.markdown('</div>', unsafe_allow_html=True)

    chat_container = st.container()
    with chat_container:
        for msg in st.session_state.ai_messages:
            is_user = msg["role"] == "user"
            align = "flex-end" if is_user else "flex-start"
            bubble_color = "#3b82f6" if is_user else ("#0d1220" if dark else "#ffffff")
            text_color = "#ffffff" if is_user else text1
            border_r = "18px 4px 18px 18px" if is_user else "4px 18px 18px 18px"
            st.markdown(f"""
            <div style="display:flex;justify-content:{align};margin-bottom:12px">
                <div style="max-width:80%;background:{bubble_color};color:{text_color};
                    padding:12px 16px;border-radius:{border_r};border:1px solid {border};font-size:14px;line-height:1.6">
                    {msg["content"]}
                </div>
            </div>
            """, unsafe_allow_html=True)

    col_inp, col_send, col_clear = st.columns([7, 1, 1])
    with col_inp:
        user_input = st.text_input("q", placeholder="Запитай про систему...", label_visibility="collapsed", key="ai_input")
    with col_send:
        send = st.button("▶", use_container_width=True)
    with col_clear:
        if st.button("🗑️", use_container_width=True):
            st.session_state.ai_messages = []
            if "ai_input" in st.session_state:
                del st.session_state["ai_input"]
            st.rerun()

    if "ai_pending" in st.session_state and st.session_state["ai_pending"]:
        pending = st.session_state.pop("ai_pending")
        st.session_state.ai_messages.append({"role": "user", "content": pending})

    if (send or user_input) and user_input:
        st.session_state.ai_messages.append({"role": "user", "content": user_input})
        if "ai_input" in st.session_state:
            del st.session_state["ai_input"]

        with st.spinner("🤖 Думаю..."):
            try:
                import requests as req
                history = []
                for msg in st.session_state.ai_messages[:-1]:
                    history.append({"role": "user" if msg["role"] == "user" else "model", "parts": [{"text": msg["content"]}]})
                contents = []
                if not history:
                    contents.append({"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\nПитання: " + user_input}]})
                else:
                    contents = history
                    contents.append({"role": "user", "parts": [{"text": user_input}]})
                if len(contents) == 1:
                    contents = [{"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + contents[0]["parts"][0]["text"]}]}]
                else:
                    contents[0]["parts"][0]["text"] = SYSTEM_PROMPT + "\n\nКонтекст надано вище.\n\n" + contents[0]["parts"][0]["text"]

                resp = req.post(
                    f"https://generativelanguage.googleapis.com/v1/models/{gemini_model}:generateContent?key={gemini_key}",
                    json={"contents": contents}, timeout=30
                )
                if resp.status_code == 200:
                    answer = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                else:
                    answer = f"❌ API помилка {resp.status_code}: {resp.text[:300]}"
            except Exception as e:
                answer = f"❌ Помилка: {e}"

        st.session_state.ai_messages.append({"role": "assistant", "content": answer})
        st.rerun()


def page_architecture():
    now = now_kyiv()
    st.markdown(f"""
    <div class="page-header">
        <div class="page-title">🗺️ Архітектура системи</div>
        <div class="page-sub">ETL Pipeline · Amazon SP-API · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="stat-card"><h4>⚡ ASYNC ЛОАДЕРИ — POST → Черга → Collector</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:2">
        <div style="color:#3b82f6;font-weight:700;margin-bottom:8px">run_ads_forever.py (планувальник)</div>
        <div style="margin-left:16px">
            <div>📊 sales_traffic_loader &nbsp;&nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>📒 ledger_summary_loader &nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>📒 ledger_detail_loader &nbsp;&nbsp;&nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>🎁 promotions_loader &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>🔄 fba_returns_loader &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>🔁 fba_replacements_loader &nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>🚚 shipments_fulfilled &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
            <div>📊 brand_analytics_loader &nbsp;→ <span style="color:#f59e0b">pending_reports</span> → collector → БД</div>
        </div>
    </div>
    </div>

    <div class="stat-card"><h4>🔄 SYNC ЛОАДЕРИ — прямі виклики API</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:2">
        <div style="margin-left:16px">
            <div>🛒 all_orders_loader &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ Orders API → БД</div>
            <div>🏭 inventory_health_loader &nbsp;→ Reports API (швидкий) → БД</div>
            <div>📋 manage_fba_loader &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ Reports API (швидкий) → БД</div>
            <div>💰 transactions_loader &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ Finance API пагінація → БД</div>
            <div>📦 fba_inbound_loader &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ FulfillmentInbound API → БД</div>
            <div>🔍 data_quality_checker &nbsp;&nbsp;&nbsp;→ перевірка якості → Telegram алерт</div>
        </div>
    </div>
    </div>

    <div class="stat-card"><h4>🎯 ADS ЛОАДЕРИ — паралельно через asyncio</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:2">
        <div style="margin-left:16px">
            <div>master_ads_loader.py</div>
            <div style="margin-left:24px">
                <div>🎯 sp_loader_4h_senior → Ads API → api_ad.sp_campaign_api</div>
                <div>🧲 sb_loader_4h_senior → Ads API → api_ad.sb_campaign_api</div>
                <div>📡 sd_loader_4h_senior → Ads API → api_ad.sd_campaign_api</div>
            </div>
        </div>
    </div>
    </div>

    <div class="stat-card"><h4>🧵 DAEMON THREADS</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:2">
        <div style="margin-left:16px">
            <div>📥 report_collector &nbsp;&nbsp;→ кожні 15 хв перевіряє pending_reports</div>
            <div>🖥️ system_monitor &nbsp;&nbsp;&nbsp;&nbsp;→ кожні 30с пише метрики сервера</div>
            <div>🤖 telegram_bot &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ приймає команди</div>
            <div>🔔 alerts_bot &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;→ моніторинг алертів</div>
        </div>
    </div>
    </div>

    <div class="stat-card"><h4>🗄️ POSTGRESQL (Heroku)</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:2">
        <div style="margin-left:16px">
            <div><span style="color:#3b82f6">csv.*</span> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;— shipments, promotions, inventory, transactions</div>
            <div><span style="color:#8b5cf6">spapi.*</span> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;— orders, returns, ledger, sales_traffic, bulk_daily</div>
            <div><span style="color:#f59e0b">api_ad.*</span> &nbsp;&nbsp;&nbsp;&nbsp;— SP / SB / SD campaigns</div>
            <div><span style="color:#22c55e">public.*</span> &nbsp;&nbsp;&nbsp;&nbsp;— etl_log, pending_reports, system_metrics</div>
            <div><span style="color:#f43f5e">brand_analytics.*</span> — search terms, ASIN analytics</div>
        </div>
    </div>
    </div>

    <div class="stat-card"><h4>📅 РОЗКЛАД v4.2 (Kyiv TZ)</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:1.8">
        <div style="color:{text4};margin-bottom:6px;font-size:11px">— NIGHT & MORNING CORE —</div>
        <div>00:45 🎁 promotions</div>
        <div>01:00 🔍 rank_tracker</div>
        <div>02:00 🎯 ads (30д)</div>
        <div>03:30 📒 ledger_summary (2д)</div>
        <div>03:35 📒 ledger_detail (7д)</div>
        <div>04:30 📦 bulk_daily</div>
        <div>05:00 🛒 all_orders (7д)</div>
        <div>06:00 🎯 ads (7д)</div>
        <div>06:30 📊 brand_analytics</div>
        <div>07:00 🚚 shipments</div>
        <div>07:30 📦 fba_inbound</div>
        <div style="color:#22c55e">08:00 🔍 data_quality ← The Guard</div>
        <div style="color:{text4};margin-top:6px;margin-bottom:6px;font-size:11px">— MORNING RUSH —</div>
        <div>09:00 🏭 inventory</div>
        <div>09:20 📊 sales_traffic (3д)</div>
        <div>09:30 💰 transactions (7д)</div>
        <div>10:00 🎯 ads (7д)</div>
        <div>10:05 🔔 alerts</div>
        <div>10:10 📋 manage_fba</div>
        <div>10:45 🎁 promotions</div>
        <div>11:00 🔄 fba_returns</div>
        <div>11:20 🔁 fba_replacements</div>
        <div style="color:{text4};margin-top:6px;margin-bottom:6px;font-size:11px">— DAYTIME SYNC —</div>
        <div>13:10 🏢 awd_inventory</div>
        <div>13:20 📊 sales_traffic (5д)</div>
        <div>13:40 🛒 all_orders (2д)</div>
        <div>14:00 🎯 ads (7д)</div>
        <div>14:10 💰 transactions (7д)</div>
        <div>14:45 🎁 promotions</div>
        <div>15:00 🏭 inventory</div>
        <div>16:10 📊 sales_traffic (7д)</div>
        <div>16:20 💰 transactions (60д)</div>
        <div>16:30 📋 manage_fba</div>
        <div>17:10 🏢 awd_inventory</div>
        <div>18:00 🎯 ads (7д)</div>
        <div>19:10 🏢 awd_inventory</div>
        <div>19:45 🎁 promotions</div>
        <div style="color:{text4};margin-top:6px;margin-bottom:6px;font-size:11px">— EVENING CLEANUP —</div>
        <div>21:00 🏭 inventory</div>
        <div>22:00 📋 manage_fba</div>
        <div>23:30 🛒 all_orders (30д)</div>
    </div>
    </div>

    <div class="stat-card"><h4>📁 СТРУКТУРА ФАЙЛІВ</h4>
    <div style="font-family:JetBrains Mono,monospace;font-size:12px;color:{text2};line-height:1.8">
        <div style="color:#3b82f6">project/</div>
        <div style="margin-left:16px">
            <div style="color:#f59e0b">├── SPAPI_Workers/</div>
            <div style="margin-left:32px;color:{text4}">
                <div>├── utils/db_queue.py</div>
                <div>├── spapi_base.py</div>
                <div>├── report_collector.py</div>
                <div>├── sales_traffic_loader.py</div>
                <div>├── ledger_detail_loader.py</div>
                <div>├── ledger_summary_loader.py</div>
                <div>├── promotions_loader.py</div>
                <div>├── fba_returns_loader.py</div>
                <div>├── fba_replacements_loader.py</div>
                <div>├── shipments_fulfilled_loader.py</div>
                <div>├── brand_analytics_loader.py</div>
                <div>├── all_orders_loader.py</div>
                <div>├── inventory_health_loader.py</div>
                <div>├── transactions_loader.py</div>
                <div>├── data_quality_checker.py</div>
                <div>└── fba_inbound_shipments_loader.py</div>
            </div>
            <div style="color:{text2}">├── run_ads_forever.py</div>
            <div style="color:{text2}">├── master_ads_loader.py</div>
            <div style="color:{text2}">├── telegram_notifier.py</div>
            <div style="color:{text2}">├── system_monitor.py</div>
            <div style="color:#22c55e">└── .env</div>
        </div>
    </div>
    </div>
    """, unsafe_allow_html=True)


# ============================================================
# MAIN
# ============================================================
def main():
    if "page" not in st.session_state:
        st.session_state.page = "📊 Статус"
    render_sidebar()
    page = st.session_state.page
    if page == "📊 Статус":
        page_status(load_all())
    elif page == "📈 Аналітика":
        page_analytics()
    elif page == "🗄️ База даних":
        page_database()
    elif page == "🖥️ Система":
        page_system()
    elif page == "🤖 AI":
        page_ai()
    elif page == "🗺️ Архітектура":
        page_architecture()
    else:
        page_status(load_all())

main()
