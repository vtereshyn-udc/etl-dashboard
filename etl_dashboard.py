"""
ETL Monitor v4.1
- Sidebar навігація
- Uptime воркерів (% успішних запусків)
- Теплова карта
- Алерти в Telegram
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
# РОЗКЛАД
# ============================================================
SCHEDULE = [
    (2,  0,  "ads",              30),
    (6,  0,  "ads",              7),
    (10, 0,  "ads",              7),
    (14, 0,  "ads",              7),
    (18, 0,  "ads",              7),
    (4,  30, "bulk_daily",       None),
    (10, 5,  "alerts",           None),
    (9,  0,  "inventory",        None),
    (15, 0,  "inventory",        None),
    (21, 0,  "inventory",        None),
    (9,  30, "transactions",     7),
    (14, 10, "transactions",     7),
    (16, 20, "transactions",     60),
    (7,  0,  "shipments",        None),
    (5,  0,  "all_orders",       7),
    (13, 40, "all_orders",       2),
    (23, 30, "all_orders",       30),
    (10, 10, "manage_fba",       None),
    (16, 30, "manage_fba",       None),
    (22, 0,  "manage_fba",       None),
    (13, 10, "awd_inventory",    None),
    (17, 10, "awd_inventory",    None),
    (19, 10, "awd_inventory",    None),
    (9,  20, "sales_traffic",    3),
    (13, 20, "sales_traffic",    5),
    (16, 10, "sales_traffic",    7),
    (0,  45, "promotions",       None),
    (10, 45, "promotions",       None),
    (14, 45, "promotions",       None),
    (19, 45, "promotions",       None),
    (11, 0,  "fba_returns",      None),
    (11, 20, "fba_replacements", None),
    (1,  0,  "rank_tracker",     None),
    (3,  30, "ledger_summary",   2),
    (3,  35, "ledger_detail",    7),
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
]

ALERT_THRESHOLDS = {
    "ads": 6, "transactions": 10, "all_orders": 10,
    "inventory": 30, "manage_fba": 30, "awd_inventory": 30,
    "shipments": 26, "sales_traffic": 10, "promotions": 8,
    "fba_returns": 26, "fba_replacements": 26, "rank_tracker": 26,
    "ledger_summary": 26, "ledger_detail": 26, "bulk_daily": 26,
}

CUSTOM_THRESHOLDS = {
    "inventory": (28, 52), "manage_fba": (28, 52), "awd_inventory": (28, 52),
    "rank_tracker": (28, 52), "ledger_summary": (28, 52), "ledger_detail": (28, 52),
    "bulk_daily": (28, 52), "shipments": (36, 72), "fba_replacements": (36, 72),
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
    text1, text2, text3, text4 = "#0f172a", "#475569", "#1e293b", "#94a3b8"
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

/* Sidebar */
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
.sb-sub {{ font-size:10px; color:{text4} !important; font-family:'JetBrains Mono',monospace; margin-top:2px; }}
.sb-divider {{ border:none; border-top:1px solid {border}; margin:12px 0; }}

.nav-item {{
    display:flex; align-items:center; gap:10px;
    padding:9px 16px; border-radius:8px; margin:2px 8px;
    cursor:pointer; font-size:13px; font-weight:500;
    color:{text2}; transition:background .15s;
    text-decoration:none;
}}
.nav-item:hover {{ background:{"rgba(255,255,255,.05)" if dark else "rgba(0,0,0,.04)"}; }}
.nav-item.active {{
    background:{"rgba(59,130,246,.15)" if dark else "rgba(59,130,246,.1)"};
    color:{'#60a5fa' if dark else '#2563eb'} !important;
}}
.nav-icon {{ font-size:16px; width:20px; text-align:center; }}
.nav-section {{ font-size:10px; color:{text4}; text-transform:uppercase; letter-spacing:.1em; padding:8px 16px 4px; font-weight:600; }}

/* Header */
.page-header {{ margin-bottom:20px; }}
.page-title {{ font-size:22px; font-weight:700; color:{text1}; letter-spacing:-.3px; }}
.page-sub {{ font-size:12px; color:{text4}; font-family:'JetBrains Mono',monospace; margin-top:2px; }}

/* Metrics */
.metrics-row {{ display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:18px; }}
.metric {{ background:{bg2}; border:1px solid {border}; border-radius:10px; padding:15px 18px; position:relative; overflow:hidden; }}
.metric::after {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; }}
.m-ok::after {{ background:#22c55e; }} .m-warn::after {{ background:#f59e0b; }}
.m-stale::after {{ background:#ef4444; }} .m-total::after {{ background:#3b82f6; }}
.metric-num {{ font-size:36px; font-weight:700; font-family:'JetBrains Mono',monospace; line-height:1; }}
.m-ok .metric-num {{ color:#22c55e; }} .m-warn .metric-num {{ color:#f59e0b; }}
.m-stale .metric-num {{ color:#ef4444; }} .m-total .metric-num {{ color:#3b82f6; }}
.metric-lbl {{ font-size:10px; color:{text4}; text-transform:uppercase; letter-spacing:.1em; margin-top:4px; font-weight:600; }}

/* Table */
.etl-wrap {{ background:{bg2}; border:1px solid {border}; border-radius:12px; overflow:hidden; margin-top:10px; }}
.etl-table {{ width:100%; border-collapse:collapse; font-size:13px; }}
.etl-table th {{ background:{th_bg}; color:{text3}; font-size:10px; text-transform:uppercase; letter-spacing:.1em; padding:10px 14px; text-align:left; font-weight:700; border-bottom:1px solid {border}; }}
.etl-table td {{ padding:11px 14px; border-bottom:1px solid {bg3}; vertical-align:middle; }}
.etl-table tr:last-child td {{ border-bottom:none; }}
.etl-table tr:hover td {{ background:{row_hover}; }}
.c-name {{ color:{text1}; font-weight:600; font-size:14px; }}
.c-tbl  {{ color:#7ec8a0; font-family:'JetBrains Mono',monospace; font-size:12px; font-weight:500; }}
.c-cnt  {{ color:{text2}; font-family:'JetBrains Mono',monospace; font-size:12px; }}
.c-ran  {{ color:#4a9e6b; font-family:'JetBrains Mono',monospace; font-size:11px; }}
.c-nxt  {{ color:#5a7a9e; font-family:'JetBrains Mono',monospace; font-size:11px; }}
.c-ok {{ color:#22c55e; font-weight:500; }} .c-warn {{ color:#f59e0b; font-weight:500; }}
.c-stale {{ color:#ef4444; font-weight:500; }} .c-empty {{ color:{text4}; }}
.freq-pill {{ font-size:10px; color:{text4}; background:{bg3}; border:1px solid {border}; padding:2px 8px; border-radius:10px; font-family:'JetBrains Mono',monospace; white-space:nowrap; }}
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

/* Analytics cards */
.stat-card {{ background:{bg2}; border:1px solid {border}; border-radius:12px; padding:18px 20px; margin-bottom:14px; }}
.stat-card h4 {{ font-size:11px; color:{text4}; text-transform:uppercase; letter-spacing:.1em; margin:0 0 14px; font-weight:700; }}
.alert-row {{ background:rgba(239,68,68,.07); border:1px solid rgba(239,68,68,.2); border-radius:8px; padding:10px 14px; margin-bottom:6px; display:flex; justify-content:space-between; align-items:center; }}
.alert-name {{ color:#ef4444; font-weight:600; font-size:13px; }}
.alert-info {{ color:{text2}; font-size:11px; font-family:'JetBrains Mono',monospace; }}

/* Uptime bars */
.uptime-row {{ display:flex; align-items:center; gap:12px; margin-bottom:8px; }}
.uptime-label {{ color:{text2}; font-size:12px; font-family:'JetBrains Mono',monospace; min-width:130px; }}
.uptime-bar-wrap {{ flex:1; height:8px; background:{"#1a2235" if dark else "#e2e8f0"}; border-radius:4px; overflow:hidden; }}
.uptime-pct {{ font-size:12px; font-weight:600; min-width:44px; text-align:right; font-family:'JetBrains Mono',monospace; }}

/* Heatmap */
.hm-wrap {{ overflow-x:auto; padding-bottom:4px; }}
.hm-table {{ border-collapse:separate; border-spacing:2px; font-size:11px; font-family:'JetBrains Mono',monospace; }}
.hm-table td.cell {{ width:16px; height:16px; border-radius:3px; }}
.hm-label {{ color:{text2}; padding-right:10px; text-align:right; white-space:nowrap; font-size:11px; min-width:110px; }}
.hm-date-label {{ color:{text4}; font-size:10px; padding:0 2px; text-align:left; }}

.etl-footer {{ text-align:center; font-size:11px; color:{text4}; margin-top:16px; font-family:'JetBrains Mono',monospace; }}
#MainMenu,footer,header,.stDeployButton {{ display:none !important; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# DB
# ============================================================

@st.cache_resource
def get_conn():
    db_url = st.secrets["DATABASE_URL"]
    return psycopg2.connect(
        db_url.replace("postgres://", "postgresql://", 1),
        sslmode='require', connect_timeout=10
    )

def query(sql, params=None):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    except Exception:
        try:
            st.cache_resource.clear()
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return cur.fetchall()
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
    r = query(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
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
            <div class="sb-sub">{now.strftime('%H:%M')} Kyiv · v4.1</div>
        </div>
        <hr class="sb-divider">
        """, unsafe_allow_html=True)

        page = st.radio(
            "nav",
            options=["📊 Статус", "📈 Аналітика"],
            label_visibility="collapsed",
            key="nav_radio"
        )

        st.markdown("<hr class='sb-divider'>", unsafe_allow_html=True)

        theme_label = "☀️ Світла тема" if dark else "🌙 Темна тема"
        if st.button(theme_label, use_container_width=True):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()

        # Статус системи в sidebar
        data = load_all()
        ok_n = sum(1 for r in data if r["status"] == "ok")
        prob_n = sum(1 for r in data if r["status"] in ("warn", "stale"))
        st.markdown(f"""
        <hr class="sb-divider">
        <div class="nav-section">Система</div>
        <div style="padding:8px 16px;font-size:12px;font-family:'JetBrains Mono',monospace">
            <div style="color:#22c55e;margin-bottom:4px">✅ OK: {ok_n}</div>
            <div style="color:{'#ef4444' if prob_n > 0 else '#3a4a6b'}">{'⚠️' if prob_n > 0 else '●'} Проблем: {prob_n}</div>
        </div>
        """, unsafe_allow_html=True)

        return page

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

    # ── Uptime воркерів
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
                    <div style="font-size:10px;color:{text4};font-family:'JetBrains Mono',monospace;min-width:50px">{count} runs</div>
                </div>"""

            st.markdown(bars_html, unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="color:{text4};font-size:13px">Немає даних в etl_log</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    # ── Алерти
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
                info = f"{h_ago:.0f}г без запуску (поріг: {threshold_h}г)" if h_ago else f"ніколи не запускався"
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
                _send_tg_alert("\n".join(lines))
        else:
            st.markdown(f'<div style="color:#22c55e;font-size:14px;padding:8px 0">✅ Всі воркери в нормі</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    # ── Теплова карта
    st.markdown('<div class="stat-card"><h4>🗓️ Теплова карта запусків (останні 30 днів)</h4>', unsafe_allow_html=True)

    if not df.empty:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(29, -1, -1)]
        all_tasks = sorted(df["task_type"].unique())

        pivot_dict = {}
        for _, row in df.groupby(["task_type", "date"]).size().reset_index(name="runs").iterrows():
            pivot_dict[(row["task_type"], row["date"])] = row["runs"]

        def cell_color(runs):
            if runs == 0:
                return "#1a2235" if dark else "#ebedf0"
            elif runs == 1: return "#1a5c3a"
            elif runs <= 3: return "#26a148"
            elif runs <= 5: return "#2ecc71"
            else: return "#57e89c"

        # Заголовки дат
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
            {"".join(f'<div style="width:14px;height:14px;background:{c};border-radius:2px"></div>' for c in (["#1a2235" if dark else "#ebedf0", "#1a5c3a", "#26a148", "#2ecc71", "#57e89c"]))}
            <span>Більше</span>
        </div>"""

        st.markdown(f'<div class="hm-wrap"><table class="hm-table">{table_rows}</table>{legend}</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="color:{text4};font-size:13px;padding:8px 0">Дані з\'являться після першого запуску воркерів.</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="etl-footer">cache 2хв · 30 днів · Kyiv TZ</div>', unsafe_allow_html=True)

def _send_tg_alert(text):
    import requests as req
    bot_token = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
    if not bot_token:
        st.warning("⚠️ TELEGRAM_BOT_TOKEN не знайдено в secrets")
        return
    db_url = st.secrets.get("DATABASE_URL", "")
    try:
        conn = psycopg2.connect(db_url.replace("postgres://", "postgresql://", 1), sslmode='require', connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM telegram_subscribers WHERE subscribed = TRUE LIMIT 10")
        subs = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
    except:
        subs = []
    sent = sum(1 for cid in subs if req.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": cid, "text": text, "parse_mode": "HTML"}, timeout=10).status_code == 200)
    st.success(f"✅ Надіслано {sent} підписникам!") if sent else st.warning("⚠️ Не вдалось надіслати")

# ============================================================
# MAIN
# ============================================================

def main():
    page = render_sidebar()
    data = load_all()

    if page == "📊 Статус":
        page_status(data)
    else:
        page_analytics()

main()
