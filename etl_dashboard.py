"""
ETL Monitor v2.0
Amazon Data Pipeline — Status Dashboard
"""

import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime, date, timedelta
import pytz

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="ETL Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================
# TIMEZONE
# ============================================================

KYIV_TZ = pytz.timezone("Europe/Kyiv")

def now_kyiv():
    return datetime.now(KYIV_TZ)

# ============================================================
# РОЗКЛАД — синхронізовано з run_forever.py
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

# task_type → (таблиця, конфлікт ключ, іконка, назва)
TASK_MAP = {
    "ads":              ("amazon_ads_sp",         "campaign_id",      "🎯", "ADS Campaigns"),
    "bulk_daily":       ("fba_shipment_items",     "shipment_id",      "📦", "FBA Inbound"),
    "inventory":        ("fba_inventory",          "sku",              "🏭", "FBA Inventory"),
    "transactions":     ("finance_events",         "transaction_id",   "💰", "Finance Events"),
    "shipments":        ("fba_shipments",          "amazon_order_id",  "🚚", "Shipments"),
    "all_orders":       ("orders",                 "amazon_order_id",  "🛒", "Orders"),
    "manage_fba":       ("manage_fba_inventory",   "sku",              "📋", "Manage FBA"),
    "awd_inventory":    ("spapi_awd_inventory",    "sku",              "🏢", "AWD Inventory"),
    "sales_traffic":    ("spapi_sales_traffic",    "date",             "📊", "Sales & Traffic"),
    "promotions":       ("promotions",             "promotion_id",     "🎁", "Promotions"),
    "fba_returns":      ("fba_returns",            "order_id",         "🔄", "FBA Returns"),
    "fba_replacements": ("fba_replacements",       "order_id",         "🔁", "FBA Replacements"),
    "rank_tracker":     ("rank_tracker",           "keyword",          "🔍", "Rank Tracker"),
    "ledger_summary":   ("spapi_ledger_summary",   "date",             "📒", "Ledger Summary"),
    "ledger_detail":    ("spapi_ledger_detail",    "date",             "📒", "Ledger Detail"),
}

# ============================================================
# РОЗКЛАД HELPERS
# ============================================================

def get_schedule_for_task(task_type):
    """Повертає всі слоти для task_type"""
    slots = []
    for h, m, t, p in SCHEDULE:
        if t == task_type:
            slots.append((h, m))
    return slots

def get_next_run(task_type):
    """Наступний запуск по Києву"""
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
    if count == 1:
        return "1× / день"
    elif count <= 4:
        return f"{count}× / день"
    else:
        return f"{count}× / день"

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
        except Exception as e:
            return None

def table_exists(table):
    r = query("SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name=%s)", (table,))
    return r and r[0][0]

def get_stats(table):
    """(count, last_date)"""
    if not table_exists(table):
        return None, None

    r = query(f"SELECT COUNT(*) FROM {table}")
    count = r[0][0] if r else 0

    last_date = None
    for col in ["snapshot_date", "report_date", "date", "updated_at", "created_at", "inserted_at"]:
        r2 = query(
            "SELECT column_name FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
            (table, col)
        )
        if r2:
            r3 = query(f"SELECT MAX({col}) FROM {table}")
            if r3 and r3[0][0]:
                last_date = r3[0][0]
                break

    return count, last_date

# ============================================================
# СТАТУС
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
    slots = get_schedule_for_task(task_type)
    if not slots:
        return "unknown"
    runs_per_day = len(slots)
    # Очікуваний інтервал + буфер 50%
    expected_hours = (24 / runs_per_day) * 1.5
    h = hours_since(last_date)
    if h is None:
        return "empty"
    if h <= expected_hours:
        return "ok"
    elif h <= expected_hours * 2.5:
        return "warn"
    else:
        return "stale"

def fmt_last(last_date):
    if last_date is None:
        return "—"
    h = hours_since(last_date)
    if h < 1:
        return f"{int(h*60)}хв тому"
    elif h < 24:
        return f"{int(h)}г тому"
    elif h < 48:
        return "вчора"
    else:
        return f"{int(h/24)}д тому"

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
        return f"{time_str} (через {m}хв)"
    return f"{time_str} (через {h}г {m:02d}хв)"

# ============================================================
# CSS
# ============================================================

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@400;500;600;700&display=swap');

* { box-sizing: border-box; }

.stApp {
    background: #080b12;
    font-family: 'Inter', sans-serif;
}

/* Заголовок */
.hdr {
    display: flex;
    align-items: baseline;
    gap: 12px;
    margin-bottom: 2px;
}
.hdr-title {
    font-size: 28px;
    font-weight: 700;
    color: #f0f4ff;
    letter-spacing: -0.5px;
}
.hdr-version {
    font-size: 11px;
    color: #2d3a52;
    font-family: 'JetBrains Mono', monospace;
    background: #0f1520;
    padding: 2px 8px;
    border-radius: 4px;
    border: 1px solid #1a2235;
}
.hdr-sub {
    font-size: 13px;
    color: #3a4a6b;
    margin-bottom: 28px;
    font-family: 'JetBrains Mono', monospace;
}

/* Метрики */
.metrics-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 24px;
}
.metric {
    background: #0d1220;
    border: 1px solid #151e30;
    border-radius: 10px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
}
.metric::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
}
.metric.ok::before    { background: #22c55e; }
.metric.warn::before  { background: #f59e0b; }
.metric.stale::before { background: #ef4444; }
.metric.total::before { background: #3b82f6; }

.metric-num {
    font-size: 40px;
    font-weight: 700;
    line-height: 1;
    font-family: 'JetBrains Mono', monospace;
}
.metric.ok    .metric-num { color: #22c55e; }
.metric.warn  .metric-num { color: #f59e0b; }
.metric.stale .metric-num { color: #ef4444; }
.metric.total .metric-num { color: #3b82f6; }

.metric-lbl {
    font-size: 11px;
    color: #3a4a6b;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 6px;
    font-weight: 600;
}

/* Таблиця */
.etl-wrap {
    background: #0d1220;
    border: 1px solid #151e30;
    border-radius: 12px;
    overflow: hidden;
}
.etl-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}
.etl-table th {
    background: #080b12;
    color: #2d3a52;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    padding: 12px 16px;
    text-align: left;
    font-weight: 600;
    border-bottom: 1px solid #151e30;
}
.etl-table td {
    padding: 13px 16px;
    border-bottom: 1px solid #0f1520;
    color: #8892a4;
    vertical-align: middle;
}
.etl-table tr:last-child td { border-bottom: none; }
.etl-table tr:hover td { background: #0f1724; }

.td-name {
    color: #c8d3e8 !important;
    font-weight: 500;
    font-size: 14px !important;
}
.td-table {
    color: #2d3a52 !important;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px !important;
}
.td-count {
    color: #4a5568 !important;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px !important;
}
.td-next {
    color: #3a4a6b !important;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px !important;
}

/* Badges */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.b-ok    { background: rgba(34,197,94,0.1);  color: #22c55e; border: 1px solid rgba(34,197,94,0.2); }
.b-warn  { background: rgba(245,158,11,0.1); color: #f59e0b; border: 1px solid rgba(245,158,11,0.2); }
.b-stale { background: rgba(239,68,68,0.1);  color: #ef4444; border: 1px solid rgba(239,68,68,0.2); }
.b-empty { background: rgba(75,85,99,0.1);   color: #4b5563; border: 1px solid rgba(75,85,99,0.2); }

/* Dot */
.dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    display: inline-block;
}
.dot-ok    { background: #22c55e; box-shadow: 0 0 6px #22c55e; }
.dot-warn  { background: #f59e0b; box-shadow: 0 0 6px #f59e0b; }
.dot-stale { background: #ef4444; box-shadow: 0 0 6px #ef4444; }
.dot-empty { background: #4b5563; }

/* Last seen color */
.last-ok    { color: #22c55e !important; }
.last-warn  { color: #f59e0b !important; }
.last-stale { color: #ef4444 !important; }
.last-empty { color: #4b5563 !important; }

/* Freq pill */
.freq {
    font-size: 10px;
    color: #2d3a52;
    background: #0f1520;
    border: 1px solid #151e30;
    padding: 2px 8px;
    border-radius: 10px;
    font-family: 'JetBrains Mono', monospace;
}

/* Toolbar */
.toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
}
.toolbar-left { font-size: 12px; color: #2d3a52; font-family: 'JetBrains Mono', monospace; }

/* Footer */
.footer {
    text-align: center;
    font-size: 11px;
    color: #1a2235;
    margin-top: 20px;
    font-family: 'JetBrains Mono', monospace;
}

/* Hide streamlit UI */
#MainMenu, footer, header, .stDeployButton { display: none !important; }
.block-container { padding-top: 2rem !important; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# ЗБІР ДАНИХ
# ============================================================

@st.cache_data(ttl=120)
def load_all_data():
    results = []
    for task_type, (table, key, icon, label) in TASK_MAP.items():
        count, last_date = get_stats(table)
        status = get_status(task_type, last_date) if count is not None else "empty"
        results.append({
            "task":      task_type,
            "icon":      icon,
            "label":     label,
            "table":     table,
            "count":     count,
            "last_date": last_date,
            "last_seen": fmt_last(last_date),
            "next_run":  fmt_next(task_type),
            "freq":      get_runs_per_day(task_type),
            "status":    status,
        })
    return results

# ============================================================
# RENDER
# ============================================================

def main():
    now = now_kyiv()

    # Заголовок
    st.markdown(f"""
        <div class="hdr">
            <span class="hdr-title">⚡ ETL Monitor</span>
            <span class="hdr-version">v2.0</span>
        </div>
        <div class="hdr-sub">Amazon Data Pipeline · {now.strftime('%Y-%m-%d %H:%M')} Kyiv</div>
    """, unsafe_allow_html=True)

    # Дані
    data = load_all_data()

    # Лічильники
    ok_n    = sum(1 for r in data if r["status"] == "ok")
    warn_n  = sum(1 for r in data if r["status"] == "warn")
    stale_n = sum(1 for r in data if r["status"] == "stale")
    empty_n = sum(1 for r in data if r["status"] == "empty")
    prob_n  = warn_n + stale_n

    # Метрики
    st.markdown(f"""
    <div class="metrics-row">
        <div class="metric ok">
            <div class="metric-num">{ok_n}</div>
            <div class="metric-lbl">✅ OK</div>
        </div>
        <div class="metric warn">
            <div class="metric-num">{prob_n}</div>
            <div class="metric-lbl">⚠️ Проблеми</div>
        </div>
        <div class="metric stale">
            <div class="metric-num">{empty_n}</div>
            <div class="metric-lbl">⬜ Порожні</div>
        </div>
        <div class="metric total">
            <div class="metric-num">{len(data)}</div>
            <div class="metric-lbl">📋 Всього</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Toolbar
    col1, col2 = st.columns([4, 1])
    with col1:
        filter_opt = st.selectbox(
            "filter",
            ["Всі таблиці", "✅ OK", "⚠️ Проблеми", "⬜ Порожні"],
            label_visibility="collapsed"
        )
    with col2:
        if st.button("⟳ Refresh", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    # Фільтр
    filtered = data
    if filter_opt == "✅ OK":
        filtered = [r for r in data if r["status"] == "ok"]
    elif filter_opt == "⚠️ Проблеми":
        filtered = [r for r in data if r["status"] in ("warn", "stale")]
    elif filter_opt == "⬜ Порожні":
        filtered = [r for r in data if r["status"] == "empty"]

    # Сортування — проблемні зверху
    order = {"stale": 0, "warn": 1, "empty": 2, "ok": 3}
    filtered.sort(key=lambda x: order.get(x["status"], 9))

    # Таблиця
    badge_html = {
        "ok":    '<span class="badge b-ok"><span class="dot dot-ok"></span>OK</span>',
        "warn":  '<span class="badge b-warn"><span class="dot dot-warn"></span>Увага</span>',
        "stale": '<span class="badge b-stale"><span class="dot dot-stale"></span>Застарів</span>',
        "empty": '<span class="badge b-empty"><span class="dot dot-empty"></span>Порожня</span>',
    }
    last_class = {"ok": "last-ok", "warn": "last-warn", "stale": "last-stale", "empty": "last-empty"}

    rows = ""
    for r in filtered:
        count_str = f"{r['count']:,}" if r["count"] is not None else "—"
        badge = badge_html.get(r["status"], "")
        lc = last_class.get(r["status"], "")
        rows += f"""
        <tr>
            <td class="td-name">{r['icon']} {r['label']}</td>
            <td class="td-table">{r['table']}</td>
            <td class="td-count">{count_str}</td>
            <td class="{lc}">{r['last_seen']}</td>
            <td class="td-next">{r['next_run']}</td>
            <td><span class="freq">{r['freq']}</span></td>
            <td>{badge}</td>
        </tr>
        """

    st.markdown(f"""
    <div class="etl-wrap">
        <table class="etl-table">
            <thead>
                <tr>
                    <th>Модуль</th>
                    <th>Таблиця</th>
                    <th>Рядків</th>
                    <th>Останнє оновлення</th>
                    <th>Наступний запуск</th>
                    <th>Частота</th>
                    <th>Статус</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
    </div>
    """, unsafe_allow_html=True)

    # Footer
    st.markdown(f"""
    <div class="footer">
        auto-refresh every 2 min · {len(filtered)} of {len(data)} tables shown · Kyiv TZ
    </div>
    """, unsafe_allow_html=True)


main()
