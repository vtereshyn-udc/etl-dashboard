"""
ETL Monitor v3.0 — FINAL
All table names verified from source code
"""

import streamlit as st
import psycopg2
from datetime import datetime, date, timedelta
import pytz

st.set_page_config(
    page_title="ETL Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

KYIV_TZ = pytz.timezone("Europe/Kyiv")

def now_kyiv():
    return datetime.now(KYIV_TZ)

# ============================================================
# РОЗКЛАД (синхронізовано з run_forever.py v3.4)
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

# ============================================================
# TASK MAP — всі назви таблиць перевірені з вихідного коду
# ============================================================
# (task_type, schema.table, date_column, icon, label)
TASK_MAP = [
    # ADS
    ("ads",              "api_ad.sp_campaign_api",        "date",          "🎯", "ADS SP"),
    ("ads",              "api_ad.sb_campaign_api",        "date",          "🧲", "ADS SB"),
    ("ads",              "api_ad.sd_campaign_api",        "date",          "📡", "ADS SD"),
    # INVENTORY
    ("inventory",        "csv.fba_inventory",             "snapshot_date", "🏭", "FBA Inventory Health"),
    ("manage_fba",       "csv.manage_fba_inventory",      "date",          "📋", "Manage FBA"),
    ("awd_inventory",    "csv.inventory",                 "date",          "🏢", "AWD + FBA Inventory"),
    # ORDERS & SHIPMENTS
    ("all_orders",       "spapi.all_orders",              "purchase_date", "🛒", "All Orders"),
    ("shipments",        "csv.fulfilled_shipments",       "shipment_date", "🚚", "Fulfilled Shipments"),
    ("bulk_daily",       "spapi.full_bulk_daily",         "date",          "📦", "Bulk Daily"),
    # FINANCE
    ("transactions",     "csv.transaction",               "date_time",     "💰", "Transactions"),
    ("fba_returns",      "spapi.fba_returns",             "return_date",   "🔄", "FBA Returns"),
    ("fba_replacements", "csv.replacements",              "shipment_date", "🔁", "FBA Replacements"),
    # ANALYTICS
    ("sales_traffic",    "spapi.sales_traffic_report",    "date",          "📊", "Sales & Traffic"),
    ("promotions",       "csv.promotions",                "shipment_date", "🎁", "Promotions"),
    ("rank_tracker",     "amzudc.rank_tracker",           "date",          "🔍", "Rank Tracker"),
    # LEDGER
    ("ledger_summary",   "spapi.ledger_summary",          "report_date",   "📒", "Ledger Summary"),
    ("ledger_detail",    "spapi.ledger_detail",           "event_date",    "📒", "Ledger Detail"),
]

# ============================================================
# SCHEDULE HELPERS
# ============================================================

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

def get_runs_per_day(task_type):
    count = sum(1 for _, _, t, _ in SCHEDULE if t == task_type)
    return f"{count}× / день"

def fmt_next(task_type):
    nxt = get_next_run(task_type)
    if not nxt:
        return "—"
    now = now_kyiv()
    diff = (nxt - now).total_seconds()
    h = int(diff // 3600)
    m = int((diff % 3600) // 60)
    time_str = nxt.strftime('%H:%M')
    if h == 0:
        return f"{time_str} ({m}хв)"
    return f"{time_str} ({h}г {m:02d}хв)"

# ============================================================
# DB
# ============================================================

@st.cache_resource
def get_conn():
    db_url = st.secrets["DATABASE_URL"]
    return psycopg2.connect(
        db_url.replace("postgres://", "postgresql://", 1),
        sslmode='require',
        connect_timeout=10
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
    r = query(
        "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)",
        (schema, table)
    )
    return r and r[0][0]

def get_stats(schema_table, date_col):
    """Повертає (count, last_date)"""
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
# STATUS
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

CUSTOM_THRESHOLDS = {
    "inventory":        (28, 52),
    "manage_fba":       (28, 52),
    "awd_inventory":    (28, 52),
    "rank_tracker":     (28, 52),
    "ledger_summary":   (36, 72),
    "ledger_detail":    (36, 72),
    "bulk_daily":       (28, 52),
    "shipments":        (36, 72),
    "fba_replacements": (36, 72),
}

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
    if h <= ok_h:
        return "ok"
    elif h <= warn_h:
        return "warn"
    else:
        return "stale"

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

# ============================================================
# LOAD DATA
# ============================================================


@st.cache_data(ttl=120)
def load_etl_log():
    """Читає останній запуск кожного task з etl_log"""
    r = query("""
        SELECT DISTINCT ON (task_type)
            task_type, ran_at, rows_saved, elapsed_sec, status
        FROM public.etl_log
        ORDER BY task_type, ran_at DESC
    """)
    if not r:
        return {}
    log = {}
    for task_type, ran_at, rows_saved, elapsed_sec, status in r:
        log[task_type] = {
            "ran_at": ran_at,
            "rows_saved": rows_saved,
            "elapsed_sec": elapsed_sec,
            "status": status,
        }
    return log


def fmt_ran_at(ran_at):
    if not ran_at:
        return "—"
    now = datetime.now()
    ran = ran_at.replace(tzinfo=None) if hasattr(ran_at, 'tzinfo') and ran_at.tzinfo else ran_at
    diff = (now - ran).total_seconds()
    if diff < 60:
        return f"{int(diff)}с тому"
    elif diff < 3600:
        return f"{int(diff/60)}хв тому"
    elif diff < 86400:
        return f"{int(diff/3600)}г тому"
    else:
        return f"{int(diff/86400)}д тому"

@st.cache_data(ttl=120)
def load_all():
    rows = []
    etl_log = load_etl_log()
    for task_type, schema_table, date_col, icon, label in TASK_MAP:
        count, last_date = get_stats(schema_table, date_col)
        status = get_status(task_type, last_date) if count is not None else "empty"
        rows.append({
            "task":      task_type,
            "icon":      icon,
            "label":     label,
            "table":     schema_table,
            "count":     count,
            "last_str":  fmt_last(last_date),
            "next_str":  fmt_next(task_type),
            "freq":      get_runs_per_day(task_type),
            "status":    status,
            "ran_at":    fmt_ran_at(etl_log.get(task_type, {}).get("ran_at")),
            "rows_run":  etl_log.get(task_type, {}).get("rows_saved") or 0,
        })
    return rows

# ============================================================
# THEME
# ============================================================

if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True

dark = st.session_state.dark_mode

if dark:
    bg, bg2, bg3 = "#080b12", "#0d1220", "#0f1520"
    border, border2 = "#151e30", "#1a2a40"
    text1, text2, text3, text4 = "#ffffff", "#d0d8e8", "#ffffff", "#6b7e9f"
    row_hover, th_bg = "#0f1724", "#080b12"
    sel_bg = "#0d1220"
else:
    bg, bg2, bg3 = "#f4f6fb", "#ffffff", "#f8f9fc"
    border, border2 = "#e2e8f0", "#cbd5e1"
    text1, text2, text3, text4 = "#0f172a", "#475569", "#94a3b8", "#cbd5e1"
    row_hover, th_bg = "#f1f5f9", "#f8f9fc"
    sel_bg = "#ffffff"

glow = "box-shadow: 0 0 5px" if dark else ""

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@400;500;600;700&display=swap');
* {{ box-sizing: border-box; }}
.stApp {{ background: {bg} !important; font-family: 'Inter', sans-serif; }}
.block-container {{ padding-top: 1.2rem !important; max-width: 1400px; }}

.etl-logo {{ display:flex; align-items:center; gap:12px; margin-bottom:2px; }}
.etl-logo img {{ height:30px; filter:{'brightness(0) invert(1)' if dark else 'none'}; opacity:{'.85' if dark else '1'}; }}
.etl-title {{ font-size:21px; font-weight:700; color:{text1}; letter-spacing:-0.3px; }}
.etl-ver {{ font-size:10px; color:{text4}; font-family:'JetBrains Mono',monospace; background:{bg3}; padding:2px 7px; border-radius:4px; border:1px solid {border}; margin-left:6px; }}
.etl-sub {{ font-size:12px; color:{text3}; margin-bottom:18px; font-family:'JetBrains Mono',monospace; }}

.metrics-row {{ display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:18px; }}
.metric {{ background:{bg2}; border:1px solid {border}; border-radius:10px; padding:15px 18px; position:relative; overflow:hidden; }}
.metric::after {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; }}
.m-ok::after {{ background:#22c55e; }}
.m-warn::after {{ background:#f59e0b; }}
.m-stale::after {{ background:#ef4444; }}
.m-total::after {{ background:#3b82f6; }}
.metric-num {{ font-size:36px; font-weight:700; font-family:'JetBrains Mono',monospace; line-height:1; }}
.m-ok .metric-num {{ color:#22c55e; }}
.m-warn .metric-num {{ color:#f59e0b; }}
.m-stale .metric-num {{ color:#ef4444; }}
.m-total .metric-num {{ color:#3b82f6; }}
.metric-lbl {{ font-size:10px; color:{text3}; text-transform:uppercase; letter-spacing:.1em; margin-top:4px; font-weight:600; }}

.etl-wrap {{ background:{bg2}; border:1px solid {border}; border-radius:12px; overflow:hidden; margin-top:10px; }}
.etl-table {{ width:100%; border-collapse:collapse; font-size:13px; }}
.etl-table th {{ background:{th_bg}; color:{text3}; font-size:10px; text-transform:uppercase; letter-spacing:.1em; padding:10px 14px; text-align:left; font-weight:600; border-bottom:1px solid {border}; }}
.etl-table td {{ padding:11px 14px; border-bottom:1px solid {bg3}; vertical-align:middle; }}
.etl-table tr:last-child td {{ border-bottom:none; }}
.etl-table tr:hover td {{ background:{row_hover}; }}

.c-name {{ color:{text1}; font-weight:600; font-size:14px; }}
.c-tbl  {{ color:#7ec8a0; font-family:'JetBrains Mono',monospace; font-size:12px; font-weight:500; }}
.c-cnt  {{ color:{text2}; font-family:'JetBrains Mono',monospace; font-size:12px; }}
.c-ran  {{ color:#4a9e6b; font-family:'JetBrains Mono',monospace; font-size:11px; }}
.c-nxt  {{ color:#5a7a9e; font-family:'JetBrains Mono',monospace; font-size:11px; }}
.c-ok   {{ color:#22c55e; font-weight:500; }}
.c-warn {{ color:#f59e0b; font-weight:500; }}
.c-stale{{ color:#ef4444; font-weight:500; }}
.c-empty{{ color:#4a5a7b; }}

.freq-pill {{ font-size:10px; color:{text3}; background:{bg3}; border:1px solid {border}; padding:2px 8px; border-radius:10px; font-family:'JetBrains Mono',monospace; white-space:nowrap; }}

.badge {{ display:inline-flex; align-items:center; gap:4px; padding:3px 9px; border-radius:20px; font-size:11px; font-weight:600; white-space:nowrap; }}
.b-ok    {{ background:rgba(34,197,94,.1);  color:#22c55e; border:1px solid rgba(34,197,94,.25); }}
.b-warn  {{ background:rgba(245,158,11,.1); color:#f59e0b; border:1px solid rgba(245,158,11,.25); }}
.b-stale {{ background:rgba(239,68,68,.1);  color:#ef4444; border:1px solid rgba(239,68,68,.25); }}
.b-empty {{ background:rgba(100,116,139,.1);color:{text3}; border:1px solid {border}; }}

.dot {{ width:6px; height:6px; border-radius:50%; display:inline-block; }}
.dot-ok    {{ background:#22c55e; {'box-shadow:0 0 5px #22c55e;' if dark else ''} }}
.dot-warn  {{ background:#f59e0b; {'box-shadow:0 0 5px #f59e0b;' if dark else ''} }}
.dot-stale {{ background:#ef4444; {'box-shadow:0 0 5px #ef4444;' if dark else ''} }}
.dot-empty {{ background:{text4}; }}

.etl-footer {{ text-align:center; font-size:11px; color:{text4}; margin-top:14px; font-family:'JetBrains Mono',monospace; }}
#MainMenu,footer,header,.stDeployButton {{ display:none !important; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# RENDER
# ============================================================

def main():
    now = now_kyiv()

    col_logo, col_toggle = st.columns([6, 1])
    with col_logo:
        st.markdown(f"""
        <div class="etl-logo">
            <img src="https://udcparts.com/cdn/shop/files/logo.svg?v=1701894617&width=300" alt="UDC">
            <span class="etl-title">ETL Monitor <span class="etl-ver">v3.0</span></span>
        </div>
        <div class="etl-sub">Amazon Data Pipeline · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
        """, unsafe_allow_html=True)
    with col_toggle:
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button("☀️ День" if dark else "🌙 Ніч", use_container_width=True):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()

    data = load_all()

    ok_n    = sum(1 for r in data if r["status"] == "ok")
    warn_n  = sum(1 for r in data if r["status"] == "warn")
    stale_n = sum(1 for r in data if r["status"] == "stale")
    empty_n = sum(1 for r in data if r["status"] == "empty")
    prob_n  = warn_n + stale_n

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

    order = {"stale": 0, "warn": 1, "empty": 2, "ok": 3}
    filtered.sort(key=lambda x: order.get(x["status"], 9))

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
        badge = badge_map.get(r["status"], "")
        lc = last_cls.get(r["status"], "c-empty")
        rows_html += f"""<tr>
            <td class="c-name">{r['icon']} {r['label']}</td>
            <td class="c-tbl">{r['table']}</td>
            <td class="c-cnt">{cnt}</td>
            <td class="{lc}">{r['last_str']}</td>
            <td class="c-ran">{r['ran_at']}</td>
            <td class="c-nxt">{r['next_str']}</td>
            <td><span class="freq-pill">{r['freq']}</span></td>
            <td>{badge}</td>
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

main()
