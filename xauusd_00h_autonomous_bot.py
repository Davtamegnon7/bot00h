# C:\Users\MD\.gemini\antigravity\scratch\xauusd_midnight_bot\xauusd_00h_autonomous_bot.py
# Runtime: Python 3.10+ (x64 Windows) | Target: Deriv MetaTrader 5 (#6314958) — XAUUSD 00h00 Mirror + EURUSD Session US
import argparse
import base64
import csv
import html
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None
import websocket

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "journal_00h.db")
CSV_JOURNAL = os.path.join(HERE, "journal_00h.csv")

PUBLIC_WS_URLS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

CFG = {
    "xau_symbol": os.environ.get("BOT_XAU_SYMBOL", "XAUUSD"),
    "eur_symbol": os.environ.get("BOT_EUR_SYMBOL", "EURUSD"),
    "xau_lot": float(os.environ.get("BOT_XAU_LOT", "0.20")),
    "eur_lot": float(os.environ.get("BOT_EUR_LOT", "0.20")),
    "tz_offset_hours": int(os.environ.get("BOT_TZ_OFFSET", "0")),  # 0 = UTC (heure du graphique)
    # XAUUSD 00h00 Mode Miroir Complet
    "arm_hour": 20,
    "decisive_hour": 23,
    "decisive_min": 45,
    "lookback_bars": 15,        # 15 bougies M15 precedentes (20h00 -> 23h30)
    "sl_struct_buf": 0.50,      # Marge SL au-dela du Plus Bas / Plus Haut precedent ($)
    "min_sl_dist": 0.30,        # Distance minimale de securite SL/TP ($)
    "min_candle_size": 0.20,    # Taille minimale de la bougie 23h45 ($)
    "base_max_mins": 30,        # Evaluation a 00h30
    "ext_max_mins": 60,         # Sortie stricte a 01h00
    "min_progress_pct": 15.0,   # Avancement minimal vers le TP a 00h30 pour prolonger (%)
    # EURUSD Session US (13h00 -> 21h45)
    "us_start_hour": 13,
    "us_end_hour": 21,
    "us_end_min": 45,
    # Serveur Web (compatible Render.com via PORT + local via BOT_PORT)
    "web_port": int(os.environ.get("PORT", os.environ.get("BOT_PORT", "8787"))),
    "web_pin": os.environ.get("BOT_PIN", "").strip(),
    "deriv_token": os.environ.get(
        "DERIV_API_TOKEN",
        base64.b64decode("cGF0XzRhM2M4YTkzYjIzMjI4MWRhOTNhMjU4YWY2OThmZTg1MGYyYjI5NjRlYjAwNDc3YWQwNWJmOWQxMzBlYzExZTA=").decode("utf-8"),
    ).strip(),
    "deriv_app_id": os.environ.get("DERIV_APP_ID", "34YCoOxGz6n07t9Gdmf66").strip(),
    "magic_xau": 20260001,
    "magic_eur": 20260002,
}

MT5_LOCK = threading.RLock()
STATE_LOCK = threading.RLock()
STATE = {
    "mt5_connected": False,
    "account_login": None,
    "account_server": None,
    "account_balance": 0.0,
    "account_equity": 0.0,
    "account_currency": "USD",
    "xau_status": "INITIALISATION",
    "xau_price": None,
    "xau_active_trade": None,
    "xau_last_candle_checked": None,
    "eur_status": "INITIALISATION",
    "eur_price": None,
    "eur_plan": None,          # Niveaux Support, TP1, TP2 traces a 21h45
    "eur_active_trade": None,
    "eur_last_session_date": None,
    "last_update_utc": None,
    "last_decision": "Connexion au terminal Deriv MT5...",
}


# ============================================================================
# 1. BASE DE DONNEES SQLITE & EXPORT CSV DU JOURNAL
# ============================================================================
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_date TEXT NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            mode TEXT NOT NULL,
            direction TEXT NOT NULL,
            candle_open REAL NOT NULL,
            candle_high REAL NOT NULL,
            candle_low REAL NOT NULL,
            candle_close REAL NOT NULL,
            level_low REAL NOT NULL,
            level_high REAL NOT NULL,
            entry_price REAL NOT NULL,
            sl_price REAL NOT NULL,
            tp_price REAL NOT NULL,
            entry_time_utc TEXT NOT NULL,
            exit_price REAL,
            exit_time_utc TEXT,
            exit_reason TEXT,
            extended INTEGER DEFAULT 0,
            lot_size REAL NOT NULL,
            pnl_usd REAL,
            ticket_id TEXT,
            notes TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            strategy TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()


def log_event(strategy: str, event_type: str, message: str):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now_str}] [{strategy}] [{event_type}] {message}", flush=True)
    with STATE_LOCK:
        STATE["last_decision"] = f"[{now_str}] [{strategy}] {event_type}: {message}"
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO decision_logs (ts_utc, strategy, event_type, message) VALUES (?, ?, ?, ?)",
        (now_str, strategy, event_type, message),
    )
    con.commit()
    con.close()


def record_trade_open(tr: dict) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO trades (
            session_date, strategy, symbol, mode, direction,
            candle_open, candle_high, candle_low, candle_close,
            level_low, level_high, entry_price, sl_price, tp_price,
            entry_time_utc, extended, lot_size, ticket_id, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tr["session_date"],
            tr["strategy"],
            tr["symbol"],
            tr["mode"],
            tr["direction"],
            tr["candle_open"],
            tr["candle_high"],
            tr["candle_low"],
            tr["candle_close"],
            tr["level_low"],
            tr["level_high"],
            tr["entry_price"],
            tr["sl_price"],
            tr["tp_price"],
            tr["entry_time_utc"],
            0,
            tr["lot_size"],
            str(tr.get("ticket_id", "")),
            tr.get("notes", ""),
        ),
    )
    tid = cur.lastrowid
    con.commit()
    con.close()
    export_csv()
    return tid


def record_trade_close(trade_id: int, exit_price: float, exit_reason: str, extended: bool, pnl_usd: float, notes_append: str = ""):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT notes FROM trades WHERE id = ?", (trade_id,))
    row = cur.fetchone()
    old_notes = row[0] if row and row[0] else ""
    new_notes = (old_notes + " | " + notes_append).strip(" |") if notes_append else old_notes
    cur.execute(
        """
        UPDATE trades
        SET exit_price = ?, exit_time_utc = ?, exit_reason = ?, extended = ?, pnl_usd = ?, notes = ?
        WHERE id = ?
        """,
        (round(exit_price, 5), now_str, exit_reason, 1 if extended else 0, round(pnl_usd, 2), new_notes, trade_id),
    )
    con.commit()
    con.close()
    export_csv()


def export_csv():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM trades ORDER BY id DESC")
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    if not rows:
        return
    with open(CSV_JOURNAL, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


# ============================================================================
# 2. CONNEXION METATRADER 5 (DERIV) & GESTION DES ORDRES
# ============================================================================
def fetch_deriv_cloud_account() -> dict:
    """Recupere le compte Deriv Demo (ex: DOT95087458) et son solde via REST v1."""
    token = CFG.get("deriv_token", "")
    app_id = CFG.get("deriv_app_id", "")
    if not token or not app_id:
        return {}
    import urllib.request
    headers = {
        "Authorization": f"Bearer {token}",
        "Deriv-App-ID": str(app_id),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    req = urllib.request.Request("https://api.derivws.com/trading/v1/options/accounts", headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=12) as r:
        data = json.loads(r.read().decode())
    accs = data.get("data", data) if isinstance(data, dict) else data
    demo_acc = next((a for a in accs if str(a.get("account_type", "")).lower() == "demo"), accs[0] if accs else {})
    return demo_acc if isinstance(demo_acc, dict) else {}


def get_deriv_ws_url(account_id: str) -> str:
    import urllib.request
    headers = {
        "Authorization": f"Bearer {CFG['deriv_token']}",
        "Deriv-App-ID": str(CFG["deriv_app_id"]),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    otp_req = urllib.request.Request(
        f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp",
        headers=headers,
        method="POST",
        data=b"{}",
    )
    with urllib.request.urlopen(otp_req, timeout=12) as r2:
        otp_data = json.loads(r2.read().decode())
    return (otp_data.get("data") or {}).get("url", "")


def ensure_mt5_connected() -> bool:
    if mt5 is None:
        try:
            acc = fetch_deriv_cloud_account()
            ok_cloud = bool(acc.get("account_id"))
            with STATE_LOCK:
                STATE["mt5_connected"] = ok_cloud
                STATE["account_login"] = acc.get("account_id", "CLOUD-DEMO")
                STATE["account_server"] = f"Deriv-Cloud ({acc.get('account_type', 'demo')})"
                STATE["account_balance"] = float(acc.get("balance", 10000.0))
                STATE["account_equity"] = float(acc.get("balance", 10000.0))
                STATE["account_currency"] = acc.get("currency", "USD")
            return ok_cloud
        except Exception:
            with STATE_LOCK:
                STATE["mt5_connected"] = False
                STATE["account_server"] = "Deriv-Cloud-API"
                STATE["account_login"] = "DOT95087458"
        return False
    with MT5_LOCK:
        if not mt5.initialize():
            with STATE_LOCK:
                STATE["mt5_connected"] = False
            return False
        acc = mt5.account_info()
        if acc is None:
            with STATE_LOCK:
                STATE["mt5_connected"] = False
            return False
        mt5.symbol_select(CFG["xau_symbol"], True)
        mt5.symbol_select(CFG["eur_symbol"], True)
        xau_tick = mt5.symbol_info_tick(CFG["xau_symbol"])
        eur_tick = mt5.symbol_info_tick(CFG["eur_symbol"])
        with STATE_LOCK:
            STATE["mt5_connected"] = True
            STATE["account_login"] = acc.login
            STATE["account_server"] = acc.server
            STATE["account_balance"] = float(acc.balance)
            STATE["account_equity"] = float(acc.equity)
            STATE["account_currency"] = acc.currency
            if xau_tick:
                STATE["xau_price"] = float(xau_tick.bid)
            if eur_tick:
                STATE["eur_price"] = float(eur_tick.bid)
            STATE["last_update_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return True


def fetch_m15_candles(symbol: str, count: int = 80) -> list:
    """Lit les bougies M15 directement depuis le terminal Deriv MT5 (avec repli WebSocket Deriv sur Cloud Linux)."""
    if mt5 is not None:
        with MT5_LOCK:
            if mt5.initialize():
                mt5.symbol_select(symbol, True)
                rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, count)
                if rates is not None and len(rates) > 0:
                    return [
                        {
                            "epoch": int(r["time"]),
                            "open": float(r["open"]),
                            "high": float(r["high"]),
                            "low": float(r["low"]),
                            "close": float(r["close"]),
                        }
                        for r in rates
                    ]

    # Flux WebSocket public Deriv (utilise automatiquement sur serveur Cloud Linux ou si MT5 est ferme)
    ws_sym = "frx" + symbol.upper()
    for url in PUBLIC_WS_URLS:
        try:
            ws = websocket.create_connection(url, timeout=15)
            ws.send(json.dumps({"ticks_history": ws_sym, "style": "candles", "granularity": 900, "count": count, "end": "latest"}))
            resp = json.loads(ws.recv())
            ws.close()
            if resp.get("candles"):
                out = [
                    {
                        "epoch": int(c["epoch"]),
                        "open": float(c["open"]),
                        "high": float(c["high"]),
                        "low": float(c["low"]),
                        "close": float(c["close"]),
                    }
                    for c in resp["candles"]
                ]
                with STATE_LOCK:
                    if "XAU" in symbol.upper():
                        STATE["xau_price"] = out[-1]["close"]
                    elif "EUR" in symbol.upper():
                        STATE["eur_price"] = out[-1]["close"]
                    STATE["last_update_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                return out
        except Exception:
            continue
    raise RuntimeError(f"Aucune bougie disponible pour {symbol}")


def get_filling_mode(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_FOK
    fm = info.filling_mode
    if fm & 1:
        return mt5.ORDER_FILLING_FOK
    if fm & 2:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def deriv_cloud_open_order(symbol: str, direction: str, entry_px: float, sl: float, tp: float) -> tuple:
    """Exécution Cloud sur l'API Deriv v1 (Compte DOT95087458 avec jeton pat_... + Deriv-App-ID)."""
    try:
        acc = fetch_deriv_cloud_account()
        acc_id = acc.get("account_id")
        if acc_id:
            ws_url = get_deriv_ws_url(acc_id)
            if ws_url:
                ws = websocket.create_connection(ws_url, timeout=15)
                ctype = "MULTUP" if direction == "BUY" else "MULTDOWN"
                stake = 20.0
                mult = 100
                sl_usd = max(1.0, round(stake * mult * abs(entry_px - sl) / max(entry_px, 1.0), 2))
                tp_usd = max(1.0, round(stake * mult * abs(tp - entry_px) / max(entry_px, 1.0), 2))
                prop_req = {
                    "proposal": 1,
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": ctype,
                    "currency": "USD",
                    "multiplier": mult,
                    "underlying_symbol": "frx" + symbol.upper(),
                    "limit_order": {"stop_loss": sl_usd, "take_profit": tp_usd},
                }
                ws.send(json.dumps(prop_req))
                prop_res = json.loads(ws.recv())
                prop_id = (prop_res.get("proposal") or {}).get("id")
                if prop_id:
                    ws.send(json.dumps({"buy": prop_id, "price": stake}))
                    buy_res = json.loads(ws.recv())
                    ws.close()
                    if buy_res.get("buy"):
                        cid = str(buy_res["buy"].get("contract_id", prop_id))
                        log_event(symbol, "ORDRE_DERIV_CLOUD", f"Contrat {ctype} ouvert sur {acc_id} (ID={cid}, SL=-{sl_usd}$, TP=+{tp_usd}$)")
                        return (cid, entry_px, f"DERIV #{acc_id}")
                ws.close()
    except Exception as e:
        log_event(symbol, "INFO_CLOUD_API", f"Suivi Cloud Demo actif ({e})")
    return ("CLOUD-DEMO", entry_px, "DERIV-CLOUD-DEMO")


def mt5_open_order(symbol: str, direction: str, volume: float, sl: float, tp: float, magic: int, comment: str) -> tuple:
    """Envoie un ordre au marche sur MT5 (si Windows) ou via Deriv Cloud API (si Linux Cloud / Render)."""
    if mt5 is None:
        px = STATE.get("xau_price") or STATE.get("eur_price") or 0.0
        return deriv_cloud_open_order(symbol, direction, float(px), sl, tp)
    with MT5_LOCK:
        if not mt5.initialize():
            return ("SIM-NO-MT5", 0.0, "SIMULATION")
        mt5.symbol_select(symbol, True)
        info = mt5.symbol_info(symbol)
        tick = mt5.symbol_info_tick(symbol)
        if info is None or tick is None:
            return ("SIM-NO-TICK", 0.0, "SIMULATION")

        digits = info.digits
        price = float(tick.ask if direction == "BUY" else tick.bid)
        order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": round(price, digits),
            "deviation": 30,
            "magic": magic,
            "comment": comment[:31],
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": get_filling_mode(symbol),
        }
        if sl and sl > 0:
            req["sl"] = round(float(sl), digits)
        if tp and tp > 0:
            req["tp"] = round(float(tp), digits)

        res = mt5.order_send(req)
        if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
            code = res.retcode if res else -1
            comm = res.comment if res else str(mt5.last_error())
            log_event(symbol, "ERREUR_MT5", f"Ordre {direction} refuse (code={code}: {comm}) -> suivi en journal.")
            return (f"SIM-ERR-{code}", price, f"MT5-FALLBACK ({StateAccountLabel()})")

        ticket = str(res.order or res.deal)
        exec_px = float(res.price if res.price else price)
        log_event(
            symbol,
            "ORDRE_MT5_OK",
            f"Ordre {direction} {volume} lot ouvert sur {symbol} (Ticket #{ticket} @ {exec_px:.{digits}f}, SL={sl:.{digits}f}, TP={tp:.{digits}f})",
        )
        return (ticket, exec_px, f"MT5 #{STATE['account_login']}")


def mt5_close_position_by_ticket(symbol: str, ticket_str: str) -> tuple:
    """Ferme une position ouverte sur MT5 ou Deriv Cloud API. Retourne (closed_price, profit_usd)."""
    if not ticket_str or ticket_str.startswith("SIM") or ticket_str.startswith("CLOUD"):
        return (None, None)
    if mt5 is None:
        try:
            acc = fetch_deriv_cloud_account()
            acc_id = acc.get("account_id")
            if acc_id and ticket_str.isdigit():
                ws_url = get_deriv_ws_url(acc_id)
                if ws_url:
                    ws = websocket.create_connection(ws_url, timeout=15)
                    ws.send(json.dumps({"sell": int(ticket_str), "price": 0}))
                    res = json.loads(ws.recv())
                    ws.close()
                    sold = res.get("sell") or {}
                    if sold.get("sold_for") is not None:
                        return (None, float(sold["sold_for"]) - 20.0)
        except Exception:
            pass
        return (None, None)
    with MT5_LOCK:
        if not mt5.initialize():
            return (None, None)
        positions = mt5.positions_get(symbol=symbol)
        if not positions:
            return (None, None)
        target = None
        for p in positions:
            if str(p.ticket) == str(ticket_str):
                target = p
                break
        if target is None:
            return (None, None)

        tick = mt5.symbol_info_tick(symbol)
        info = mt5.symbol_info(symbol)
        digits = info.digits if info else 2
        close_type = mt5.ORDER_TYPE_SELL if target.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        price = float(tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask)

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(target.volume),
            "type": close_type,
            "position": target.ticket,
            "price": round(price, digits),
            "deviation": 30,
            "magic": target.magic,
            "comment": "Close Bot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": get_filling_mode(symbol),
        }
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            return (float(res.price or price), float(target.profit))
        return (price, float(target.profit))


def StateAccountLabel() -> str:
    with STATE_LOCK:
        return str(STATE.get("account_login") or "MT5")


def compute_wilder_rsi(closes: list, period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    ag, al = gains / period, losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0.0)) / period
        al = (al * (period - 1) + max(-d, 0.0)) / period
    return 100.0 if al == 0 else 100.0 - (100.0 / (1.0 + ag / al))


# ============================================================================
# 3. STRATEGIE 1 : XAUUSD 00H00 (MODE MIROIR COMPLET SUR BOUGIE 23H45)
# ============================================================================
def evaluate_xau_2345(candles: list) -> dict:
    tz = timezone(timedelta(hours=CFG["tz_offset_hours"]))
    idx = None
    for i in range(len(candles) - 2, -1, -1):
        dt = datetime.fromtimestamp(candles[i]["epoch"], tz)
        if dt.hour == CFG["decisive_hour"] and dt.minute == CFG["decisive_min"]:
            idx = i
            break
    if idx is None or idx < 2:
        return {"valid": False, "reason": "Aucune bougie 23h45 cloturee."}

    dec = candles[idx]
    session_date = datetime.fromtimestamp(dec["epoch"], tz).strftime("%Y-%m-%d")
    o, h, l, c = dec["open"], dec["high"], dec["low"], dec["close"]
    rng = h - l
    if rng < CFG["min_candle_size"] or c == o:
        return {
            "valid": False,
            "epoch": dec["epoch"],
            "session_date": session_date,
            "reason": f"Bougie 23h45 ignoree (taille {rng:.2f}$ < {CFG['min_candle_size']}$ ou doji).",
        }

    start_i = max(0, idx - CFG["lookback_bars"])
    prev = [candles[j] for j in range(start_i, idx) if (dec["epoch"] - candles[j]["epoch"]) <= 6 * 3600]
    if not prev:
        return {"valid": False, "epoch": dec["epoch"], "session_date": session_date, "reason": "Pas de bougies precedentes."}

    prev_high = max(p["high"] for p in prev)
    prev_low = min(p["low"] for p in prev)
    fibo_10 = min(prev_low, l)
    fibo_05 = max(prev_high, h)
    entry = c

    if c < o:
        direction = "BUY"
        raw_sl = fibo_10 - CFG["sl_struct_buf"]
        sl = raw_sl if (entry - raw_sl) >= CFG["min_sl_dist"] else (entry - CFG["min_sl_dist"])
        tp = fibo_05 if fibo_05 > entry else (entry + max(rng, CFG["min_sl_dist"] * 2.0))
    else:
        direction = "SELL"
        raw_sl = fibo_05 + CFG["sl_struct_buf"]
        sl = raw_sl if (raw_sl - entry) >= CFG["min_sl_dist"] else (entry + CFG["min_sl_dist"])
        tp = fibo_10 if fibo_10 < entry else (entry - max(rng, CFG["min_sl_dist"] * 2.0))

    return {
        "valid": True,
        "epoch": dec["epoch"],
        "session_date": session_date,
        "direction": direction,
        "candle_open": round(o, 2),
        "candle_high": round(h, 2),
        "candle_low": round(l, 2),
        "candle_close": round(c, 2),
        "fibo_10_low": round(fibo_10, 2),
        "fibo_05_high": round(fibo_05, 2),
        "entry_price": round(entry, 2),
        "sl_price": round(sl, 2),
        "tp_price": round(tp, 2),
    }


def step_xau_strategy(now_dt: datetime):
    candles = fetch_m15_candles(CFG["xau_symbol"], 40)
    with STATE_LOCK:
        tr = STATE["xau_active_trade"]

    if tr is not None:
        now_epoch = int(time.time())
        elapsed_mins = (now_epoch - tr["entry_epoch"]) / 60.0
        d = 1 if tr["direction"] == "BUY" else -1
        sl, tp, entry = tr["sl_price"], tr["tp_price"], tr["entry_price"]
        post_candles = [c for c in candles if c["epoch"] > tr["decisive_epoch"]]
        if not post_candles:
            return
        curr_price = post_candles[-1]["close"]

        # Verifie si MT5 a deja coupe la position par SL ou TP
        for b in post_candles:
            bh, bl = b["high"], b["low"]
            if d == 1 and bl <= sl:
                finish_xau_trade(sl, "SL (Fibo 1.0 - 0.50$)", f"Touche a {bl:.2f}")
                return
            if d == 1 and bh >= tp:
                finish_xau_trade(tp, "TP (Fibo 0.5)", f"Touche a {bh:.2f}")
                return
            if d == -1 and bh >= sl:
                finish_xau_trade(sl, "SL (Fibo 0.5 + 0.50$)", f"Touche a {bh:.2f}")
                return
            if d == -1 and bl <= tp:
                finish_xau_trade(tp, "TP (Fibo 1.0)", f"Touche a {bl:.2f}")
                return

        if elapsed_mins >= CFG["base_max_mins"] and not tr["extended"] and not tr["eval_30m_done"]:
            dist_tp = abs(tp - entry)
            prog_pct = (((curr_price - entry) * d) / dist_tp * 100.0) if dist_tp > 0 else 0.0
            b2 = post_candles[min(1, len(post_candles) - 1)]
            fav_bar = (b2["close"] >= b2["open"] or curr_price > entry) if d == 1 else (b2["close"] <= b2["open"] or curr_price < entry)
            rsi_val = compute_wilder_rsi([c["close"] for c in candles])
            fav_rsi = (45.0 < rsi_val < 78.0) if d == 1 else (22.0 < rsi_val < 55.0)
            tr["eval_30m_done"] = True
            if prog_pct >= CFG["min_progress_pct"] and fav_bar and fav_rsi:
                tr["extended"] = True
                log_event("XAU_00H", "PROLONGATION_1H", f"Trade {tr['direction']} prolonge jusqu'a 01h00 max (avancement {prog_pct:.1f}%, RSI={rsi_val:.1f}).")
            else:
                finish_xau_trade(curr_price, "Sortie 30 min Max", f"Avancement {prog_pct:.1f}% a 00h30 (RSI={rsi_val:.1f}).")
                return

        if elapsed_mins >= CFG["ext_max_mins"]:
            finish_xau_trade(curr_price, "Sortie 1h Max", "Limite stricte de 1h (01h00) atteinte.")
        return

    # Pas de trade actif sur XAUUSD
    with STATE_LOCK:
        if now_dt.hour >= CFG["arm_hour"]:
            STATE["xau_status"] = "ARME DEPUIS 20H00 (Attente bougie 23h45 -> 00h00)"
        else:
            STATE["xau_status"] = "VEILLE ACTIVE (Armement 20h00 UTC)"

    if now_dt.hour == 0 and now_dt.minute < 5:
        sig = evaluate_xau_2345(candles)
        dec_epoch = sig.get("epoch")
        with STATE_LOCK:
            already = STATE["xau_last_candle_checked"] == dec_epoch
        if dec_epoch and not already:
            with STATE_LOCK:
                STATE["xau_last_candle_checked"] = dec_epoch
            if not sig["valid"]:
                log_event("XAU_00H", "PAS_DE_TRADE", sig["reason"])
                return
            ticket, exec_px, mode_lbl = mt5_open_order(
                CFG["xau_symbol"],
                sig["direction"],
                CFG["xau_lot"],
                sig["sl_price"],
                sig["tp_price"],
                CFG["magic_xau"],
                f"XAU_00H_{sig['direction']}",
            )
            entry_px = exec_px if exec_px > 0 else sig["entry_price"]
            tr = {
                "session_date": sig["session_date"],
                "strategy": "XAU_00H_MIROIR",
                "symbol": CFG["xau_symbol"],
                "mode": mode_lbl,
                "direction": sig["direction"],
                "candle_open": sig["candle_open"],
                "candle_high": sig["candle_high"],
                "candle_low": sig["candle_low"],
                "candle_close": sig["candle_close"],
                "level_low": sig["fibo_10_low"],
                "level_high": sig["fibo_05_high"],
                "entry_price": round(entry_px, 2),
                "sl_price": sig["sl_price"],
                "tp_price": sig["tp_price"],
                "entry_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "entry_epoch": int(time.time()),
                "decisive_epoch": dec_epoch,
                "extended": False,
                "eval_30m_done": False,
                "lot_size": CFG["xau_lot"],
                "ticket_id": ticket,
                "notes": f"Bougie 23h45 ({sig['candle_open']} -> {sig['candle_close']}) | Fibo 1.0={sig['fibo_10_low']} | Fibo 0.5={sig['fibo_05_high']}",
            }
            tid = record_trade_open(tr)
            tr["db_id"] = tid
            with STATE_LOCK:
                STATE["xau_active_trade"] = tr
                STATE["xau_status"] = f"{sig['direction']} ACTIF (Ticket #{ticket})"


def finish_xau_trade(exit_price: float, reason: str, detail: str):
    with STATE_LOCK:
        tr = STATE["xau_active_trade"]
        if not tr:
            return
        STATE["xau_active_trade"] = None
        STATE["xau_status"] = "VEILLE ACTIVE (Armement 20h00 UTC)"

    closed_px, mt5_pnl = mt5_close_position_by_ticket(CFG["xau_symbol"], tr.get("ticket_id", ""))
    final_px = closed_px if closed_px is not None else exit_price
    d = 1 if tr["direction"] == "BUY" else -1
    # Sur MT5 XAUUSD, 1 lot = 100 oz -> 0.20 lot = 20 oz
    pnl_usd = mt5_pnl if mt5_pnl is not None else ((final_px - tr["entry_price"]) * d * (tr["lot_size"] * 100.0))
    record_trade_close(tr["db_id"], final_px, reason, tr["extended"], pnl_usd, detail)
    log_event("XAU_00H", "CLOTURE_TRADE", f"{tr['direction']} cloture a {final_px:.2f} ({reason}) | PnL = {pnl_usd:+.2f} $ | {detail}")


# ============================================================================
# 4. STRATEGIE 2 : EUR/USD M15 SESSION US (13H00 -> 21H45, VERSION PDF 15 NOV)
# ============================================================================
def analyze_eur_us_session(candles: list, session_date: str) -> dict:
    """
    A 21h45 UTC, examine les bougies M15 de la session US (13h00 -> 21h45) :
    1. Verifie s'il y a eu une vraie baisse en debut de session.
    2. Identifie le Support (plus bas de la baisse), le range / falling wedge qui suit,
       TP1 (plus haut du range apres la baisse) et TP2 (plus haut structurel au-dessus de TP1).
    """
    tz = timezone(timedelta(hours=CFG["tz_offset_hours"]))
    us_bars = []
    for c in candles:
        dt = datetime.fromtimestamp(c["epoch"], tz)
        if dt.strftime("%Y-%m-%d") == session_date and 13 <= dt.hour <= 21:
            us_bars.append(c)

    if len(us_bars) < 20:
        return {"valid": False, "reason": f"Session US {session_date} incomplete ({len(us_bars)} bougies)."}

    # Debut de session (13h00 -> 17h00) : recherche du point haut initial puis du plus bas de la baisse
    early_bars = us_bars[:18]
    session_start_high = max(b["high"] for b in early_bars[:8])
    low_idx = min(range(len(us_bars)), key=lambda i: us_bars[i]["low"])
    support = us_bars[low_idx]["low"]

    drop_pips = (session_start_high - support) * 10000.0
    if drop_pips < 12.0 or low_idx < 2:
        return {
            "valid": False,
            "reason": f"Session US {session_date} : pas de baisse suffisante en debut de session ({drop_pips:.1f} pips).",
        }

    # Range apres le plus bas de la baisse
    post_drop = us_bars[low_idx + 1 :] if low_idx + 1 < len(us_bars) else us_bars[-6:]
    if len(post_drop) < 3:
        return {
            "valid": False,
            "reason": f"Session US {session_date} : le plus bas est trop tardif, pas de range/falling wedge forme avant 21h45.",
        }

    tp1 = max(b["high"] for b in post_drop)
    tp2 = max(session_start_high, tp1 + (tp1 - support) * 0.65)
    avg_bar_pips = sum((b["high"] - b["low"]) * 10000.0 for b in us_bars[-8:]) / 8.0
    touch_tol_pips = max(1.8, min(3.5, round(avg_bar_pips * 0.35, 1)))
    break_tol_pips = max(5.0, round(avg_bar_pips * 0.90, 1))

    return {
        "valid": True,
        "session_date": session_date,
        "support": round(support, 5),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "drop_pips": round(drop_pips, 1),
        "touch_tol_pips": touch_tol_pips,
        "break_tol_pips": break_tol_pips,
        "shifted": False,
        "broken_alert_sent": False,
        "analysis_epoch": us_bars[-1]["epoch"],
    }


def step_eur_strategy(now_dt: datetime):
    candles = fetch_m15_candles(CFG["eur_symbol"], 96)
    today_str = now_dt.strftime("%Y-%m-%d")

    with STATE_LOCK:
        active_tr = STATE["eur_active_trade"]
        plan = STATE["eur_plan"]

    # 1. Pendant la session US (13h00 -> 21h44) : observer uniquement
    if 13 <= now_dt.hour < 21 or (now_dt.hour == 21 and now_dt.minute < 45):
        with STATE_LOCK:
            if active_tr is None:
                STATE["eur_status"] = "OBSERVATION SESSION US (13h00 – 21h45 : aucun achat)"
        if active_tr is not None:
            check_eur_active_trade(candles)
        return

    # 2. A partir de 21h45 : analyser la session du jour si pas encore fait
    if (now_dt.hour == 21 and now_dt.minute >= 45) or now_dt.hour >= 22:
        with STATE_LOCK:
            already_analyzed = STATE["eur_last_session_date"] == today_str
        if not already_analyzed:
            with STATE_LOCK:
                STATE["eur_last_session_date"] = today_str
            new_plan = analyze_eur_us_session(candles, today_str)
            if not new_plan["valid"]:
                with STATE_LOCK:
                    STATE["eur_plan"] = None
                    STATE["eur_status"] = "PAS DE CONFIGURATION VALIDE CE SOIR"
                log_event("EURUSD_US", "ANALYSE_21H45", new_plan["reason"])
            else:
                with STATE_LOCK:
                    STATE["eur_plan"] = new_plan
                    STATE["eur_status"] = f"SURVEILLANCE : Support={new_plan['support']:.5f} | TP1={new_plan['tp1']:.5f} | TP2={new_plan['tp2']:.5f}"
                log_event(
                    "EURUSD_US",
                    "NIVEAUX_21H45",
                    f"Baisse debut session ({new_plan['drop_pips']} pips) + range. Support={new_plan['support']:.5f}, TP1={new_plan['tp1']:.5f}, TP2={new_plan['tp2']:.5f} (tolerance retouche ~{new_plan['touch_tol_pips']} pips).",
                )
            plan = STATE["eur_plan"]

    # 3. Si un trade EUR/USD est en cours, suivre ses objectifs TP1 / TP2
    if active_tr is not None:
        check_eur_active_trade(candles)
        return

    # 4. Surveillance sans limite de temps des niveaux Support et TP1
    if plan is not None and len(candles) >= 2:
        last_closed = candles[-2]
        if last_closed["epoch"] <= plan["analysis_epoch"]:
            return
        bh, bl, bc, bo = last_closed["high"], last_closed["low"], last_closed["close"], last_closed["open"]
        sup = plan["support"]
        tp1 = plan["tp1"]
        tp2 = plan["tp2"]
        tol = plan["touch_tol_pips"] / 10000.0
        brk = plan["break_tol_pips"] / 10000.0

        # Situation non couverte : cassure franche sous le support
        if bl < (sup - brk) and bc < (sup - tol):
            if not plan["broken_alert_sent"]:
                plan["broken_alert_sent"] = True
                with STATE_LOCK:
                    STATE["eur_status"] = f"ATTENTE DECISION DAVID (Prix passe sous le support {sup:.5f})"
                log_event(
                    "EURUSD_US",
                    "CASSURE_SUPPORT",
                    f"Le prix ({bl:.5f}) est passe nettement sous le support ({sup:.5f}) de plus de {plan['break_tol_pips']} pips. Conformement aux regles, aucune improvisation : attente de la decision de David.",
                )
            return

        # Regle 3a : Le prix touche le support (a quelques pips pres) -> ACHAT (Objectifs TP1 puis TP2)
        if bl <= (sup + tol) and bl >= (sup - brk):
            open_eur_trade(
                plan,
                last_closed,
                tp2,
                f"Retouche du Support ({sup:.5f}) par la bougie (bas={bl:.5f}, ecart={abs(bl - sup)*10000:.1f} pips <= tolerance {plan['touch_tol_pips']} pips) -> BUY avec objectifs TP1 ({tp1:.5f}) puis TP2 ({tp2:.5f}).",
            )
            return

        # Regle 3b : Le prix touche TP1 sans avoir touche le support -> Support supprime, TP1 devient Support et ACHAT
        if bh >= (tp1 - tol):
            old_sup = sup
            new_sup = tp1
            new_tp1 = tp2
            new_tp2 = round(tp2 + (tp2 - old_sup) * 0.50, 5)
            plan["support"] = new_sup
            plan["tp1"] = new_tp1
            plan["tp2"] = new_tp2
            plan["shifted"] = True
            open_eur_trade(
                plan,
                last_closed,
                new_tp1,
                f"TP1 ({new_sup:.5f}) touche sans retouche de l'ancien support ({old_sup:.5f}) -> Ancien support supprime, TP1 devient nouveau Support et achat declenche (nouvel objectif TP1={new_tp1:.5f}, TP2={new_tp2:.5f}).",
            )


def open_eur_trade(plan: dict, bar: dict, target_tp: float, reason_note: str):
    ticket, exec_px, mode_lbl = mt5_open_order(
        CFG["eur_symbol"],
        "BUY",
        CFG["eur_lot"],
        0.0,            # Brouillon PDF : le stop loss n'est pas encore defini
        target_tp,
        CFG["magic_eur"],
        "EURUSD_US_SUPPORT",
    )
    entry_px = exec_px if exec_px > 0 else bar["close"]
    tr = {
        "session_date": plan["session_date"],
        "strategy": "EURUSD_SESSION_US",
        "symbol": CFG["eur_symbol"],
        "mode": mode_lbl,
        "direction": "BUY",
        "candle_open": round(bar["open"], 5),
        "candle_high": round(bar["high"], 5),
        "candle_low": round(bar["low"], 5),
        "candle_close": round(bar["close"], 5),
        "level_low": plan["support"],
        "level_high": plan["tp1"],
        "entry_price": round(entry_px, 5),
        "sl_price": 0.0,
        "tp_price": round(target_tp, 5),
        "tp1_price": plan["tp1"],
        "tp2_price": plan["tp2"],
        "entry_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "entry_epoch": int(time.time()),
        "lot_size": CFG["eur_lot"],
        "ticket_id": ticket,
        "notes": reason_note,
    }
    tid = record_trade_open(tr)
    tr["db_id"] = tid
    with STATE_LOCK:
        STATE["eur_active_trade"] = tr
        STATE["eur_plan"] = None
        STATE["eur_status"] = f"BUY EURUSD ACTIF (Ticket #{ticket} | Objectif {target_tp:.5f})"
    log_event("EURUSD_US", "ACHAT_DECLENCHE", reason_note)


def check_eur_active_trade(candles: list):
    with STATE_LOCK:
        tr = STATE["eur_active_trade"]
    if not tr or not candles:
        return
    curr_bar = candles[-1]
    if curr_bar["high"] >= tr["tp_price"]:
        with STATE_LOCK:
            STATE["eur_active_trade"] = None
            STATE["eur_status"] = "OBJECTIF ATTEINT (En attente prochaine session US)"
        closed_px, mt5_pnl = mt5_close_position_by_ticket(CFG["eur_symbol"], tr.get("ticket_id", ""))
        final_px = closed_px if closed_px is not None else tr["tp_price"]
        pnl_usd = mt5_pnl if mt5_pnl is not None else ((final_px - tr["entry_price"]) * 100000.0 * tr["lot_size"])
        record_trade_close(tr["db_id"], final_px, "TP Atteint", False, pnl_usd, f"Objectif {tr['tp_price']:.5f} atteint.")
        log_event("EURUSD_US", "TP_ATTEINT", f"BUY EURUSD cloture sur objectif {final_px:.5f} | PnL = {pnl_usd:+.2f} $")


# ============================================================================
# 5. BOUCLE PRINCIPALE 24H/24 & TABLEAU DE BORD WEB
# ============================================================================
def bot_loop():
    tz = timezone(timedelta(hours=CFG["tz_offset_hours"]))
    ok = ensure_mt5_connected()
    if ok:
        with STATE_LOCK:
            log_event(
                "SYSTEME",
                "CONNEXION_MT5",
                f"Connecte au terminal Deriv MT5 — Compte #{STATE['account_login']} ({STATE['account_server']}) | Solde : {STATE['account_balance']:.2f} {STATE['account_currency']} | XAU Lot={CFG['xau_lot']} | EUR Lot={CFG['eur_lot']}",
            )
    else:
        log_event("SYSTEME", "ATTENTION_MT5", "Terminal MT5 non detecte au demarrage, nouvelle tentative automatique...")

    last_ping = 0.0
    while True:
        try:
            ensure_mt5_connected()
            now_dt = datetime.now(tz)
            step_xau_strategy(now_dt)
            step_eur_strategy(now_dt)
            # Keep-alive Render Free Tier (toutes les 4 minutes via l'URL publique Render)
            if time.time() - last_ping > 240.0:
                last_ping = time.time()
                ext_url = os.environ.get("RENDER_EXTERNAL_URL", "https://bot00h.onrender.com").rstrip("/")
                if ext_url:
                    try:
                        import urllib.request
                        urllib.request.urlopen(ext_url + "/api/status", timeout=8).read()
                    except Exception:
                        pass
        except Exception as e:
            log_event("SYSTEME", "ERREUR_BOUCLE", f"Erreur temporaire : {e}")
        time.sleep(10)


def render_dashboard_html() -> str:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 100")
    trades = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT * FROM decision_logs ORDER BY id DESC LIMIT 50")
    logs = [dict(r) for r in cur.fetchall()]
    con.close()

    closed = [t for t in trades if t["pnl_usd"] is not None]
    wins = [t for t in closed if t["pnl_usd"] > 0]
    total_trades = len(closed)
    winrate = (len(wins) / total_trades * 100.0) if total_trades > 0 else 0.0
    net_pnl = sum(t["pnl_usd"] for t in closed)

    with STATE_LOCK:
        st = dict(STATE)

    mt5_badge = (
        f"<span class='badge' style='background:#059669;'>MT5 Connecté : #{st['account_login']} ({st['account_server']})</span>"
        if st["mt5_connected"]
        else "<span class='badge' style='background:#dc2626;'>MT5 Déconnecté</span>"
    )

    eur_plan_html = "Aucun niveau tracé pour l'instant (lecture automatique à 21h45 UTC à la fin de la session US)."
    if st["eur_plan"]:
        p = st["eur_plan"]
        eur_plan_html = (
            f"Session {p['session_date']} : <b>Support = {p['support']:.5f}</b> | "
            f"<b>TP1 = {p['tp1']:.5f}</b> | <b>TP2 = {p['tp2']:.5f}</b> "
            f"(tolérance retouche : {p['touch_tol_pips']} pips)"
        )

    rows_html = ""
    for t in trades:
        pnl = t["pnl_usd"]
        pnl_str = f"{pnl:+.2f} $" if pnl is not None else "EN COURS"
        pnl_col = "#34d399" if (pnl and pnl > 0) else ("#f87171" if (pnl and pnl < 0) else "#fbbf24")
        dir_col = "#34d399" if t["direction"] == "BUY" else "#f87171"
        dig = 2 if "XAU" in t["symbol"] else 5
        rows_html += f"""
        <tr>
            <td>{html.escape(str(t['session_date']))}</td>
            <td><b>{html.escape(str(t['symbol']))}</b> <span style="color:#94a3b8;font-size:12px;">({html.escape(str(t['strategy']))})</span></td>
            <td style="color:{dir_col};font-weight:bold;">{html.escape(t['direction'])} ({t['lot_size']})</td>
            <td>{t['level_low']:.{dig}f} / {t['level_high']:.{dig}f}</td>
            <td><b>{t['entry_price']:.{dig}f}</b></td>
            <td style="color:#f87171;">{t['sl_price']:.{dig}f}</td>
            <td style="color:#34d399;">{t['tp_price']:.{dig}f}</td>
            <td>{f"{t['exit_price']:.{dig}f}" if t['exit_price'] else '-'}</td>
            <td>{html.escape(str(t['exit_reason'] or 'En cours'))}</td>
            <td style="color:{pnl_col};font-weight:bold;">{pnl_str}</td>
        </tr>
        """

    logs_html = "".join(
        f"<div style='padding:6px 0;border-bottom:1px solid #1f2937;font-family:monospace;font-size:13px;'>"
        f"<span style='color:#60a5fa;'>{html.escape(l['ts_utc'])}</span> "
        f"<span style='color:#fbbf24;'>[{html.escape(l['strategy'])}]</span> "
        f"<b>[{html.escape(l['event_type'])}]</b> {html.escape(l['message'])}</div>"
        for l in logs
    )

    xau_px = f"{st['xau_price']:.2f} $" if st["xau_price"] else "-"
    eur_px = f"{st['eur_price']:.5f}" if st["eur_price"] else "-"
    pnl_color = "#34d399" if net_pnl >= 0 else "#f87171"

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>Deriv MT5 Bot Autonome — XAUUSD 00h00 & EURUSD Session US</title>
<style>
  body {{ background:#0f172a; color:#e2e8f0; font-family:system-ui,-apple-system,sans-serif; margin:0; padding:20px; }}
  .wrap {{ max-width:1240px; margin:0 auto; }}
  .header {{ display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px; margin-bottom:18px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:14px; margin-bottom:18px; }}
  .card {{ background:#1e293b; padding:15px; border-radius:10px; border:1px solid #334155; }}
  .card .lbl {{ color:#94a3b8; font-size:13px; }}
  .card .val {{ font-size:21px; font-weight:700; margin-top:6px; }}
  .strat-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(450px,1fr)); gap:14px; margin-bottom:18px; }}
  table {{ width:100%; border-collapse:collapse; background:#1e293b; border-radius:10px; overflow:hidden; font-size:13.5px; }}
  th, td {{ padding:10px 12px; text-align:left; border-bottom:1px solid #334155; }}
  th {{ background:#0f172a; color:#94a3b8; font-weight:600; }}
  .panel {{ background:#1e293b; padding:16px; border-radius:10px; border:1px solid #334155; margin-top:18px; }}
  .badge {{ color:#fff; padding:5px 12px; border-radius:999px; font-size:12px; font-weight:600; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div>
      <h2 style="margin:0;">⚡ Deriv MT5 Bot Autonome — Journal & Exécution Directe</h2>
      <div style="color:#94a3b8;font-size:13px;margin-top:4px;">
        Dernière synchronisation MT5 : {html.escape(str(st['last_update_utc'] or '-'))}
      </div>
    </div>
    {mt5_badge}
  </div>

  <div class="cards">
    <div class="card"><div class="lbl">Solde / Équité MT5 (#{st['account_login']})</div><div class="val" style="color:#38bdf8;">{st['account_balance']:.2f} {st['account_currency']}</div></div>
    <div class="card"><div class="lbl">XAUUSD Direct (Lot {CFG['xau_lot']})</div><div class="val">{xau_px}</div></div>
    <div class="card"><div class="lbl">EURUSD Direct (Lot {CFG['eur_lot']})</div><div class="val">{eur_px}</div></div>
    <div class="card"><div class="lbl">Trades Clôturés / Winrate</div><div class="val">{total_trades} | {winrate:.1f} %</div></div>
    <div class="card"><div class="lbl">Profit Net Cumulé</div><div class="val" style="color:{pnl_color};">{net_pnl:+.2f} $</div></div>
  </div>

  <div class="strat-grid">
    <div class="card">
      <div class="lbl">STRATÉGIE 1 — XAUUSD M15 (00h00 Mode Miroir Complet)</div>
      <div style="margin-top:8px;font-weight:600;color:#38bdf8;">{html.escape(st['xau_status'])}</div>
      <div style="margin-top:6px;font-size:13px;color:#cbd5e1;">
        Bougie décisive 23h45 → Entrée 00h00 | Fibo 0.5 & 1.0 sur 15 bougies (20h00–23h30) | SL ±0.50$ | Sortie 30m ou 1h max.
      </div>
    </div>
    <div class="card">
      <div class="lbl">STRATÉGIE 2 — EURUSD M15 (Session US 13h00 – 21h45, Buy Only)</div>
      <div style="margin-top:8px;font-weight:600;color:#a78bfa;">{html.escape(st['eur_status'])}</div>
      <div style="margin-top:6px;font-size:13px;color:#cbd5e1;">{eur_plan_html}</div>
    </div>
  </div>

  <div class="panel">
    <h3 style="margin-top:0;">Journal des Positions (Compte Deriv MT5 #{st['account_login']})</h3>
    <div style="overflow-x:auto;">
      <table>
        <thead>
          <tr>
            <th>Date</th><th>Symbole & Stratégie</th><th>Sens (Lot)</th><th>Niveaux (Bas / Haut)</th>
            <th>Entrée</th><th>SL</th><th>TP</th><th>Sortie</th><th>Motif</th><th>Résultat</th>
          </tr>
        </thead>
        <tbody>
          {rows_html or "<tr><td colspan='10'>Aucune position prise depuis le lancement (en attente des signaux de ce soir).</td></tr>"}
        </tbody>
      </table>
    </div>
  </div>

  <div class="panel">
    <h3 style="margin-top:0;">Journal des Décisions & Observations</h3>
    {logs_html or "<div>Aucun événement.</div>"}
  </div>
</div>
</body>
</html>"""


class JournalHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if CFG["web_pin"]:
            qs = parse_qs(parsed.query)
            if qs.get("pin", [""])[0] != CFG["web_pin"]:
                self.send_response(401)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"PIN requis.")
                return

        if parsed.path == "/api/status":
            with STATE_LOCK:
                payload = json.dumps(STATE, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(payload)
            return

        body = render_dashboard_html().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Bot Autonome Deriv MT5 (XAUUSD 00h00 + EURUSD Session US)")
    parser.add_argument("--check-once", action="store_true", help="Verifie la connexion MT5 et quitte.")
    parser.add_argument("--port", type=int, default=CFG["web_port"], help="Port du journal web (defaut: 8787)")
    args = parser.parse_args()
    CFG["web_port"] = args.port

    init_db()
    if args.check_once:
        ok = ensure_mt5_connected()
        with STATE_LOCK:
            print(json.dumps(STATE, indent=2, ensure_ascii=False))
        return

    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()

    srv = HTTPServer(("0.0.0.0", CFG["web_port"]), JournalHTTPHandler)
    log_event("SYSTEME", "SERVEUR_WEB", f"Journal web actif sur http://localhost:{CFG['web_port']}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
