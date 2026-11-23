#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Cloud Deriv — XAU/USD Minuit 00h00 (Fibo Mode Miroir & Multiplicateurs)
Exécution exclusive sur l'API Deriv v1 (frxXAUUSD Multiplicateurs)
Compte Démo Deriv : DOT95087458 (Token PAT + Deriv-App-ID)
Déploiement Render.com : Service autonome dédié (bot00h-gold)
Portefeuille Dédié : 100.00 $ USD | Mise calibrée : 2.00 $ (2% strict)
Quant Brain & Intelligence : Réflexion post-trade, calcul de régime, auto-adaptation
"""

import os
import sys
import time
import json
import sqlite3
import csv
import math
import threading
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    import websocket
except ImportError:
    print("[ERREUR] Module websocket manquant. Installer avec: pip install websocket-client", flush=True)
    sys.exit(1)

# ==============================================================================
# CONFIGURATION DU ROBOT XAU/USD CLOUD
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "xauusd_cloud_journal.db")
CSV_PATH = os.path.join(BASE_DIR, "xauusd_cloud_journal.csv")

CFG = {
    "web_port": int(os.environ.get("PORT", os.environ.get("BOT_PORT", "10001"))),
    "deriv_token": os.environ.get(
        "DERIV_API_TOKEN",
        "pat_4a3c8a93b232281da93a258af698fe850f2b2964eb00477ad05bf9d130ec11e0",
    ).strip(),
    "deriv_app_id": os.environ.get("DERIV_APP_ID", "34YCoOxGz6n07t9Gdmf66").strip(),
    "deriv_demo_account": os.environ.get("DERIV_ACCOUNT_ID", "DOT95087458").strip(),
    "symbol": "frxXAUUSD",
    "symbol_label": "XAU/USD (Gold Minuit)",
    "initial_capital_usd": float(os.environ.get("INITIAL_CAPITAL", "100.0")),
    "stake_usd": float(os.environ.get("XAU_STAKE", "2.0")),
    "multiplier": int(os.environ.get("XAU_MULTIPLIER", "100")),
    "lookback_bars": int(os.environ.get("LOOKBACK_BARS", "15")),
    "sl_buffer_usd": float(os.environ.get("SL_BUFFER", "0.50")),
    "smart_ext_enabled": True,
}

PUBLIC_WS_URLS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://blue.derivws.com/websockets/v3?app_id=1089",
    "wss://green.derivws.com/websockets/v3?app_id=1089",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

STATE_LOCK = threading.Lock()
STATE = {
    "bot_name": "Cloud XAU/USD Bot — Minuit Fibo Miroir",
    "environment": "Render Cloud Linux",
    "account_id": CFG["deriv_demo_account"],
    "deriv_balance": 10000.0,
    "portfolio_capital_initial": CFG["initial_capital_usd"],
    "portfolio_equity": CFG["initial_capital_usd"],
    "portfolio_total_pnl": 0.0,
    "portfolio_roi_pct": 0.0,
    "account_currency": "USD",
    "symbol": CFG["symbol"],
    "current_price": 0.0,
    "last_tick_utc": "",
    "phase": "ATTENTE_MINUIT",
    "phase_detail": "En veille. Armement automatique à 20h00 UTC, détection bougie à 23h45 UTC.",
    "session_date": "",
    "plan": None,
    "active_trade": None,
    "last_checked_utc": "",
    "candles_m15": [],
    "last_update_utc": "",
    "quant_brain": {
        "status": "IA ACTIVE & ADAPTATIVE",
        "mode": "FIBO MIROIR (Reversal Minuit)",
        "atr_m15": 0.0,
        "regime": "CALME",
        "last_reflection": "Initialisation du modèle. Surveillance de la liquidité nocturne.",
        "win_rate": 0.0,
        "total_trades": 0,
    },
}

# ==============================================================================
# BASE DE DONNEES SQLITE & JOURNAL CSV
# ==============================================================================
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS xauusd_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_date TEXT,
            symbol TEXT,
            direction TEXT,
            contract_type TEXT,
            entry_price REAL,
            candle_open REAL,
            candle_close REAL,
            fibo_1_0 REAL,
            fibo_0_5 REAL,
            sl_price REAL,
            tp_price REAL,
            stake_usd REAL,
            multiplier INTEGER,
            contract_id TEXT,
            entry_time_utc TEXT,
            exit_price REAL,
            exit_time_utc TEXT,
            exit_reason TEXT,
            pnl_usd REAL,
            portfolio_equity REAL,
            status TEXT,
            reflection_note TEXT,
            notes TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT,
            event_type TEXT,
            message TEXT
        )
        """
    )
    con.commit()
    con.close()
    recalculate_portfolio_metrics()


def log_event(event_type: str, message: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{event_type}] {message}", flush=True)
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("INSERT INTO logs (timestamp_utc, event_type, message) VALUES (?, ?, ?)", (ts, event_type, message))
        con.commit()
        con.close()
    except Exception:
        pass


def recalculate_portfolio_metrics():
    """Recalcule l'équité exacte du portefeuille 100$ à partir de tous les trades fermés."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT pnl_usd, status FROM xauusd_trades WHERE status = 'CLOSED'")
        rows = cur.fetchall()
        con.close()

        total_pnl = sum(r["pnl_usd"] for r in rows if r["pnl_usd"] is not None)
        total_trades = len(rows)
        wins = sum(1 for r in rows if (r["pnl_usd"] or 0) > 0)
        win_rate = round((wins / total_trades * 100.0), 1) if total_trades > 0 else 0.0

        with STATE_LOCK:
            init_cap = CFG["initial_capital_usd"]
            equity = round(init_cap + total_pnl, 2)
            roi = round((total_pnl / init_cap * 100.0), 2)
            STATE["portfolio_capital_initial"] = init_cap
            STATE["portfolio_total_pnl"] = round(total_pnl, 2)
            STATE["portfolio_equity"] = equity
            STATE["portfolio_roi_pct"] = roi
            STATE["quant_brain"]["total_trades"] = total_trades
            STATE["quant_brain"]["win_rate"] = win_rate
    except Exception as e:
        print(f"[RECALC_ERR] {e}", flush=True)


def has_traded_for_session(session_date: str) -> bool:
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM xauusd_trades WHERE session_date = ?", (session_date,))
        cnt = cur.fetchone()[0]
        con.close()
        return cnt > 0
    except Exception:
        return False


def record_trade_open(trade_data: dict) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO xauusd_trades (
            session_date, symbol, direction, contract_type, entry_price,
            candle_open, candle_close, fibo_1_0, fibo_0_5, sl_price, tp_price,
            stake_usd, multiplier, contract_id, entry_time_utc, status, reflection_note, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)
        """,
        (
            trade_data["session_date"],
            trade_data["symbol"],
            trade_data["direction"],
            trade_data["contract_type"],
            trade_data["entry_price"],
            trade_data.get("candle_open", 0.0),
            trade_data.get("candle_close", 0.0),
            trade_data.get("fibo_1_0", 0.0),
            trade_data.get("fibo_0_5", 0.0),
            trade_data.get("sl_price", 0.0),
            trade_data.get("tp_price", 0.0),
            trade_data["stake_usd"],
            trade_data["multiplier"],
            trade_data["contract_id"],
            trade_data["entry_time_utc"],
            trade_data.get("reflection_note", ""),
            trade_data.get("notes", ""),
        ),
    )
    tid = cur.lastrowid
    con.commit()
    con.close()
    export_csv()
    return tid


def record_trade_close(contract_id: str, exit_price: float, exit_reason: str, pnl_usd: float, reflection_note: str = ""):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE xauusd_trades
        SET exit_price = ?, exit_time_utc = ?, exit_reason = ?, pnl_usd = ?, status = 'CLOSED',
            reflection_note = CASE WHEN ? != '' THEN ? ELSE reflection_note END
        WHERE contract_id = ?
        """,
        (exit_price, ts, exit_reason, pnl_usd, reflection_note, reflection_note, str(contract_id)),
    )
    con.commit()
    con.close()
    recalculate_portfolio_metrics()
    export_csv()


def export_csv():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM xauusd_trades ORDER BY id DESC")
        rows = cur.fetchall()
        con.close()
        if rows:
            with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                for r in rows:
                    writer.writerow(dict(r))
    except Exception as e:
        print(f"[CSV_EXPORT_ERR] {e}", flush=True)


# ==============================================================================
# API DERIV V1 (OPTIONS & MULTIPLICATEURS)
# ==============================================================================
def fetch_deriv_account() -> dict:
    token = CFG.get("deriv_token", "")
    app_id = CFG.get("deriv_app_id", "")
    if not token or not app_id:
        return {}
    headers = {
        "Authorization": f"Bearer {token}",
        "Deriv-App-ID": str(app_id),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "BotCloud-XAUUSD",
    }
    try:
        req = urllib.request.Request("https://api.derivws.com/trading/v1/options/accounts", headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode())
        accs = data.get("data", data) if isinstance(data, dict) else data
        demo_acc = next((a for a in accs if str(a.get("account_type", "")).lower() == "demo"), accs[0] if accs else {})
        if demo_acc:
            with STATE_LOCK:
                STATE["account_id"] = demo_acc.get("account_id", CFG["deriv_demo_account"])
                STATE["deriv_balance"] = float(demo_acc.get("balance", STATE["deriv_balance"]))
                STATE["account_currency"] = demo_acc.get("currency", "USD")
        return demo_acc if isinstance(demo_acc, dict) else {}
    except Exception as e:
        log_event("DERIV_API_WARN", f"Erreur fetch_deriv_account: {e}")
        return {}


def get_deriv_ws_url(account_id: str) -> str:
    headers = {
        "Authorization": f"Bearer {CFG['deriv_token']}",
        "Deriv-App-ID": str(CFG["deriv_app_id"]),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "BotCloud-XAUUSD",
    }
    try:
        otp_req = urllib.request.Request(
            f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp",
            headers=headers,
            method="POST",
            data=b"{}",
        )
        with urllib.request.urlopen(otp_req, timeout=12) as r2:
            otp_data = json.loads(r2.read().decode())
        return (otp_data.get("data") or {}).get("url", "")
    except Exception as e:
        log_event("DERIV_OTP_ERR", f"Erreur obtention OTP WebSocket Deriv: {e}")
        return ""


def fetch_m15_candles(symbol: str = CFG["symbol"], count: int = 50) -> list:
    for url in PUBLIC_WS_URLS:
        try:
            ws = websocket.create_connection(url, timeout=10)
            ws.send(
                json.dumps(
                    {
                        "ticks_history": symbol,
                        "style": "candles",
                        "granularity": 900,
                        "count": count,
                        "end": "latest",
                    }
                )
            )
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
                    STATE["current_price"] = out[-1]["close"]
                    STATE["last_tick_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    STATE["candles_m15"] = out[-35:]
                return out
        except Exception:
            continue
    raise RuntimeError(f"Flux WebSocket Deriv indisponible pour {symbol}")


def deriv_open_multiplier_contract(direction: str, entry_px: float, tp_px: float, sl_px: float = 0.0) -> tuple:
    """Ouvre un contrat multiplicateur (MULTUP ou MULTDOWN) sur Gold frxXAUUSD."""
    acc_id = STATE.get("account_id") or CFG["deriv_demo_account"]
    ws_url = get_deriv_ws_url(acc_id)
    if not ws_url:
        log_event("ORDRE_REFUSE", f"OTP Deriv indisponible @ {entry_px:.2f}$")
        return (None, 0.0)

    stake = CFG["stake_usd"]
    mult = CFG["multiplier"]
    ctype = "MULTUP" if direction.upper() == "BUY" else "MULTDOWN"

    # Calcul dynamique TP / SL en USD
    tp_distance = abs(tp_px - entry_px)
    tp_usd = max(1.00, round(stake * mult * (tp_distance / max(entry_px, 1.0)), 2))
    sl_usd = min(stake, 2.00)  # Max risk strictly capped at 2.00$

    try:
        ws = websocket.create_connection(ws_url, timeout=15)
        prop_req = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": ctype,
            "currency": "USD",
            "multiplier": mult,
            "underlying_symbol": CFG["symbol"],
            "limit_order": {
                "take_profit": min(tp_usd, 50.0),
                "stop_loss": sl_usd,
            },
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
                log_event(
                    "ORDRE_DERIV_REUSSI",
                    f"Contrat {ctype} ouvert sur {acc_id} (ID={cid} | Stake={stake}$ | Mult={mult} | TP=+{tp_usd}$ | SL=-{sl_usd}$)",
                )
                fetch_deriv_account()
                return (cid, entry_px)
            else:
                log_event("ACHAT_REFUSE", f"Réponse Deriv buy: {buy_res}")
        else:
            log_event("PROPOSAL_ERR", f"Réponse Deriv proposal: {prop_res}")
            ws.close()
    except Exception as e:
        log_event("ERREUR_ORDRE_DERIV", f"Exception ordre Deriv: {e}")

    return (None, 0.0)


def check_deriv_active_contract(contract_id: str) -> dict:
    acc_id = STATE.get("account_id") or CFG["deriv_demo_account"]
    ws_url = get_deriv_ws_url(acc_id)
    if not ws_url:
        return {}
    try:
        ws = websocket.create_connection(ws_url, timeout=12)
        ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": int(contract_id)}))
        res = json.loads(ws.recv())
        ws.close()
        poc = res.get("proposal_open_contract", {})
        if poc:
            is_sold = bool(poc.get("is_sold", 0))
            profit = float(poc.get("profit", 0.0))
            status = poc.get("status", "open")
            exit_tick = float(poc.get("exit_tick", poc.get("current_spot", 0.0)))
            return {"is_open": not is_sold, "profit": profit, "status": status, "exit_price": exit_tick}
    except Exception as e:
        log_event("CHECK_CONTRACT_WARN", f"Erreur suivi contrat #{contract_id}: {e}")
    return {}


def deriv_close_contract(contract_id: str) -> tuple:
    acc_id = STATE.get("account_id") or CFG["deriv_demo_account"]
    ws_url = get_deriv_ws_url(acc_id)
    if not ws_url:
        return (False, 0.0, 0.0)
    try:
        ws = websocket.create_connection(ws_url, timeout=12)
        ws.send(json.dumps({"sell": int(contract_id), "price": 0}))
        res = json.loads(ws.recv())
        ws.close()
        sell_data = res.get("sell")
        if sell_data:
            sold_for = float(sell_data.get("sold_for", 0.0))
            pnl = round(sold_for - CFG["stake_usd"], 2)
            fetch_deriv_account()
            return (True, 0.0, pnl)
    except Exception as e:
        log_event("SELL_ERR", f"Erreur clôture contrat #{contract_id}: {e}")
    return (False, 0.0, 0.0)


# ==============================================================================
# STRATÉGIE MINUIT 00H00 (FIBO MODE MIROIR SUR GOLD)
# ==============================================================================
def compute_wilder_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0.0)
        loss = max(-diff, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_atr(candles: list, period: int = 14) -> float:
    if len(candles) < period + 1:
        return 2.50
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return round(sum(trs[-period:]) / period, 2)


def evaluate_xau_2345(candles: list) -> dict:
    target_idx = None
    for i in range(len(candles) - 1, -1, -1):
        dt = datetime.fromtimestamp(candles[i]["epoch"], timezone.utc)
        if dt.strftime("%H:%M") == "23:45":
            target_idx = i
            break

    if target_idx is None:
        return {"valid": False, "reason": "Bougie M15 de 23h45 UTC non encore clôturée"}

    bar_2345 = candles[target_idx]
    is_bullish = bar_2345["close"] >= bar_2345["open"]
    direction = "SELL" if is_bullish else "BUY"  # Mode Miroir

    lb = CFG.get("lookback_bars", 15)
    prior_bars = candles[max(0, target_idx - lb) : target_idx]
    all_eval_bars = prior_bars + [bar_2345]

    highest_high = max(b["high"] for b in all_eval_bars)
    lowest_low = min(b["low"] for b in all_eval_bars)

    fibo_1_0 = lowest_low
    fibo_0_5 = highest_high
    buffer = CFG["sl_buffer_usd"]

    if direction == "BUY":
        sl = round(fibo_1_0 - buffer, 2)
        tp = round(fibo_0_5, 2)
    else:
        sl = round(fibo_0_5 + buffer, 2)
        tp = round(fibo_1_0, 2)

    atr_val = compute_atr(candles)
    regime = "TENDANCE FORTE" if atr_val > 4.5 else ("RANGE MODÉRÉ" if atr_val > 2.0 else "FAIBLE VOLATILITÉ")

    with STATE_LOCK:
        STATE["quant_brain"]["atr_m15"] = atr_val
        STATE["quant_brain"]["regime"] = regime

    return {
        "valid": True,
        "bar_2345": bar_2345,
        "color": "Verte (Haussière)" if is_bullish else "Rouge (Baissière)",
        "direction": direction,
        "fibo_1_0": round(fibo_1_0, 2),
        "fibo_0_5": round(fibo_0_5, 2),
        "sl": sl,
        "tp": tp,
        "atr": atr_val,
        "regime": regime,
    }


def step_xau_strategy(now_dt: datetime):
    today_str = now_dt.strftime("%Y-%m-%d")
    hhmm = now_dt.strftime("%H:%M")
    sec = now_dt.second

    # A. Gestion prioritaire du trade actif (00h00 -> 01h00 UTC)
    with STATE_LOCK:
        active = STATE["active_trade"]

    if active is not None:
        manage_active_trade(now_dt, active, hhmm)
        return

    # B. Phase d'Armement (20h00 -> 23h45 UTC)
    if "20:00" <= hhmm < "23:45":
        with STATE_LOCK:
            STATE["phase"] = "ARMÉ_POUR_MINUIT"
            STATE["phase_detail"] = (
                f"Robot armé pour 00h00 UTC. Surveillance Gold spot: {STATE['current_price']:.2f}$ | "
                f"Portefeuille 100$: {STATE['portfolio_equity']:.2f}$"
            )
        return

    # C. Déclenchement à 00h00 UTC pile
    if hhmm == "00:00" and sec <= 45 and active is None:
        if has_traded_for_session(today_str):
            with STATE_LOCK:
                STATE["phase"] = "SESSION_TERMINEE"
                STATE["phase_detail"] = f"Trade déjà exécuté pour la session du {today_str}. Verrou activé."
            return

        try:
            candles = fetch_m15_candles(count=45)
        except Exception as e:
            log_event("ERREUR_FLUX", f"Échec récupération bougies: {e}")
            return

        res = evaluate_xau_2345(candles)
        if not res.get("valid"):
            with STATE_LOCK:
                STATE["phase"] = "ATTENTE_BOUGIE"
                STATE["phase_detail"] = res.get("reason", "En attente bougie 23h45 UTC")
            return

        bar = res["bar_2345"]
        dir_trade = res["direction"]
        spot = STATE["current_price"] if STATE["current_price"] > 0 else bar["close"]

        contract_id, exec_px = deriv_open_multiplier_contract(
            direction=dir_trade,
            entry_px=spot,
            tp_px=res["tp"],
            sl_px=res["sl"],
        )

        if not contract_id:
            log_event("ECHEC_ORDRE", "Impossible d'ouvrir le contrat multiplicateur sur Deriv.")
            return

        entry_px = exec_px if exec_px > 0 else spot
        reflection = (
            f"IA Signal Minuit : Bougie 23h45 {res['color']} -> Contrat Miroir {dir_trade}. "
            f"Fibo 1.0={res['fibo_1_0']}$, Fibo 0.5={res['fibo_0_5']}$. Régime: {res['regime']} (ATR={res['atr']}$). "
            f"Mise: {CFG['stake_usd']}$ (2% du portefeuille 100$)."
        )

        tr = {
            "session_date": today_str,
            "symbol": CFG["symbol"],
            "direction": dir_trade,
            "contract_type": "MULTUP" if dir_trade == "BUY" else "MULTDOWN",
            "entry_price": round(entry_px, 2),
            "candle_open": round(bar["open"], 2),
            "candle_close": round(bar["close"], 2),
            "fibo_1_0": res["fibo_1_0"],
            "fibo_0_5": res["fibo_0_5"],
            "sl_price": res["sl"],
            "tp_price": res["tp"],
            "stake_usd": CFG["stake_usd"],
            "multiplier": CFG["multiplier"],
            "contract_id": str(contract_id),
            "entry_time_utc": now_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "status": "OPEN",
            "extended": False,
            "reflection_note": reflection,
            "notes": f"Mode Miroir 00h00 | Cible TP={res['tp']}$",
        }

        record_trade_open(tr)
        with STATE_LOCK:
            STATE["active_trade"] = tr
            STATE["phase"] = "EN_COURS_MINUIT"
            STATE["phase_detail"] = f"Trade {dir_trade} actif sur Gold (Contrat #{contract_id} | Mise={CFG['stake_usd']}$)"
            STATE["quant_brain"]["last_reflection"] = reflection

        log_event("TRADE_EXECUTE", f"Gold {dir_trade} ouvert @ {entry_px:.2f}$ (Contrat #{contract_id})")
        return

    # D. En dehors des heures
    if active is None and hhmm > "01:00":
        with STATE_LOCK:
            STATE["phase"] = "REPOS"
            STATE["phase_detail"] = "Session de minuit clôturée. Prochain armement ce soir à 20h00 UTC."


def manage_active_trade(now_dt: datetime, tr: dict, hhmm: str):
    cid = tr["contract_id"]
    der_stat = check_deriv_active_contract(cid)

    # 1. Clôture automatique par Deriv (TP ou SL touché)
    if der_stat and not der_stat.get("is_open", True):
        profit = der_stat.get("profit", 0.0)
        exit_px = der_stat.get("exit_price", STATE["current_price"])
        reason = "Take Profit Atteint (+Gain)" if profit > 0 else "Stop Loss Protection (-Perte)"
        reflection = (
            f"Trade clôturé par Deriv au {reason}. PnL={profit:+.2f}$. "
            f"Capital Portefeuille: {STATE['portfolio_equity'] + profit:.2f}$."
        )
        record_trade_close(cid, exit_px, reason, profit, reflection)
        with STATE_LOCK:
            STATE["active_trade"] = None
            STATE["phase"] = "SESSION_TERMINEE"
            STATE["phase_detail"] = f"Trade #{cid} terminé : {reason} ({profit:+.2f} $)"
            STATE["quant_brain"]["last_reflection"] = reflection
        log_event("TRADE_TERMINE", f"Contrat #{cid} clôturé : {reason} | PnL={profit:+.2f}$")
        return

    # 2. Évaluation intelligente à 00h30 UTC (Prolongation ou clôture marché)
    if hhmm >= "00:30" and not tr.get("extended", False):
        try:
            candles = fetch_m15_candles(count=25)
            closes = [c["close"] for c in candles]
            rsi = compute_wilder_rsi(closes)
        except Exception:
            rsi = 50.0

        spot = STATE["current_price"]
        entry = tr["entry_price"]
        tp = tr["tp_price"]

        target_dist = abs(tp - entry)
        curr_dist = (spot - entry) if tr["direction"] == "BUY" else (entry - spot)
        progression = (curr_dist / target_dist * 100.0) if target_dist > 0 else 0.0

        is_favorable = (tr["direction"] == "BUY" and rsi >= 48.0) or (tr["direction"] == "SELL" and rsi <= 52.0)

        if progression >= 15.0 and is_favorable:
            tr["extended"] = True
            reflection = (
                f"Évaluation 00h30 : Progression favorable de {progression:.1f}% vers TP | RSI={rsi:.1f}. "
                f"Décision IA : Prolongation du trade autorisée jusqu'à 01h00 UTC."
            )
            with STATE_LOCK:
                STATE["quant_brain"]["last_reflection"] = reflection
            log_event("EVAL_00H30", reflection)
        else:
            # Clôture anticipée à 00h30 pour sécuriser
            ok, px, pnl = deriv_close_contract(cid)
            reason = f"Clôture Anticipée 00h30 (Progression insuffisante {progression:.1f}%)"
            reflection = f"Évaluation 00h30 : Momentum insuffisant. Clôture marché de sécurité. PnL={pnl:+.2f}$."
            record_trade_close(cid, spot, reason, pnl, reflection)
            with STATE_LOCK:
                STATE["active_trade"] = None
                STATE["phase"] = "SESSION_TERMINEE"
                STATE["phase_detail"] = f"Trade clôturé à 00h30 : {reason} ({pnl:+.2f} $)"
                STATE["quant_brain"]["last_reflection"] = reflection
            log_event("CLOTURE_00H30", f"Clôture anticipée contrat #{cid} | PnL={pnl:+.2f}$")
            return

    # 3. Clôture impérative à 01h00 UTC
    if hhmm >= "01:00":
        ok, px, pnl = deriv_close_contract(cid)
        spot = STATE["current_price"]
        reason = "Clôture Impérative 01h00 UTC (Fin de session)"
        reflection = f"Fin de fenêtre temporelle 01h00 UTC. Clôture stricte de position. PnL={pnl:+.2f}$."
        record_trade_close(cid, spot, reason, pnl, reflection)
        with STATE_LOCK:
            STATE["active_trade"] = None
            STATE["phase"] = "SESSION_TERMINEE"
            STATE["phase_detail"] = f"Fin de session minuit : {reason} ({pnl:+.2f} $)"
            STATE["quant_brain"]["last_reflection"] = reflection
        log_event("CLOTURE_01H00", f"Clôture stricte contrat #{cid} @ 01h00 UTC | PnL={pnl:+.2f}$")


def recover_active_trade_on_startup():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM xauusd_trades WHERE status = 'OPEN' ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        if row:
            cid = row["contract_id"]
            stat = check_deriv_active_contract(cid)
            if stat and not stat.get("is_open", True):
                profit = stat.get("profit", 0.0)
                exit_px = stat.get("exit_price", row["entry_price"])
                record_trade_close(cid, exit_px, "Réconciliation démarrage (Contrat vendu sur Deriv)", profit)
                log_event("RECONCILIATION", f"Contrat #{cid} détecté fermé sur Deriv. PnL={profit:+.2f}$")
            else:
                tr = dict(row)
                with STATE_LOCK:
                    STATE["active_trade"] = tr
                    STATE["phase"] = "EN_COURS_MINUIT"
                    STATE["phase_detail"] = f"Trade #{cid} restauré au démarrage."
                log_event("RESTAURATION", f"Trade ouvert #{cid} restauré en mémoire.")
    except Exception as e:
        print(f"[RECOVERY_ERR] {e}", flush=True)


# ==============================================================================
# SERVEUR WEB & DASHBOARD DE CONTROLE
# ==============================================================================
class XauusdDashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/status":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with STATE_LOCK:
                st = dict(STATE)
            self.wfile.write(json.dumps(st, default=str).encode("utf-8"))
            return

        if self.path == "/api/trades":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            con = sqlite3.connect(DB_PATH)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM xauusd_trades ORDER BY id DESC LIMIT 30")
            trades = [dict(r) for r in cur.fetchall()]
            con.close()
            self.wfile.write(json.dumps(trades).encode("utf-8"))
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        html = self.render_dashboard()
        self.wfile.write(html.encode("utf-8"))

    def render_dashboard(self) -> str:
        with STATE_LOCK:
            st = dict(STATE)
            qb = dict(st["quant_brain"])
            act = st.get("active_trade")
            candles = list(st.get("candles_m15", []))

        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM xauusd_trades ORDER BY id DESC LIMIT 20")
        trades = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 10")
        logs = [dict(r) for r in cur.fetchall()]
        con.close()

        # Construction du SVG des bougies
        svg_w, svg_h = 760, 260
        candles_svg = ""
        if len(candles) >= 5:
            min_p = min(c["low"] for c in candles)
            max_p = max(c["high"] for c in candles)
            rng = max(max_p - min_p, 1.0)
            pad_y = 20
            dx = svg_w / max(len(candles), 1)

            for i, c in enumerate(candles):
                x = i * dx + dx * 0.5
                y_high = pad_y + (max_p - c["high"]) / rng * (svg_h - 2 * pad_y)
                y_low = pad_y + (max_p - c["low"]) / rng * (svg_h - 2 * pad_y)
                y_open = pad_y + (max_p - c["open"]) / rng * (svg_h - 2 * pad_y)
                y_close = pad_y + (max_p - c["close"]) / rng * (svg_h - 2 * pad_y)

                is_green = c["close"] >= c["open"]
                color = "#10b981" if is_green else "#ef4444"
                top_body = min(y_open, y_close)
                body_h = max(abs(y_close - y_open), 2.0)

                candles_svg += f'<line x1="{x:.1f}" y1="{y_high:.1f}" x2="{x:.1f}" y2="{y_low:.1f}" stroke="{color}" stroke-width="1.5"/>'
                candles_svg += f'<rect x="{x - dx * 0.35:.1f}" y="{top_body:.1f}" width="{dx * 0.7:.1f}" height="{body_h:.1f}" fill="{color}" rx="1"/>'

        # Lignes du tableau des trades
        trade_rows = ""
        for t in trades:
            pnl = t.get("pnl_usd")
            pnl_str = f"{pnl:+.2f} $" if pnl is not None else "-"
            pnl_cls = "text-emerald-400 font-bold" if (pnl or 0) > 0 else ("text-rose-400 font-bold" if (pnl or 0) < 0 else "text-slate-400")
            badge_cls = "bg-emerald-500/20 text-emerald-300 border-emerald-500/40" if t["direction"] == "BUY" else "bg-rose-500/20 text-rose-300 border-rose-500/40"
            trade_rows += f"""
            <tr class="border-b border-slate-800/60 hover:bg-slate-800/30 transition text-sm">
                <td class="py-3 px-3 font-mono text-slate-400">#{t['id']}</td>
                <td class="py-3 px-3 text-slate-300">{t['session_date']}</td>
                <td class="py-3 px-3"><span class="px-2 py-0.5 rounded border text-xs font-semibold {badge_cls}">{t['direction']}</span></td>
                <td class="py-3 px-3 font-mono text-amber-300">{t['entry_price']:.2f}$</td>
                <td class="py-3 px-3 font-mono text-indigo-300">{t['tp_price']:.2f}$</td>
                <td class="py-3 px-3 font-mono text-rose-300">{t['sl_price']:.2f}$</td>
                <td class="py-3 px-3 font-mono text-slate-300">{t.get('exit_price') or '-'}{'$' if t.get('exit_price') else ''}</td>
                <td class="py-3 px-3 font-mono {pnl_cls}">{pnl_str}</td>
                <td class="py-3 px-3 text-xs text-slate-400">{t.get('exit_reason') or 'En cours'}</td>
            </tr>
            """

        active_card = """
        <div class="p-4 rounded-xl border border-slate-800 bg-slate-900/60 text-slate-400 text-sm">
            Aucun trade actif en ce moment. Surveillance active du marché en attente de minuit.
        </div>
        """
        if act:
            dir_cls = "text-emerald-400" if act["direction"] == "BUY" else "text-rose-400"
            active_card = f"""
            <div class="p-5 rounded-xl border border-amber-500/40 bg-amber-950/20 text-slate-200">
                <div class="flex items-center justify-between mb-3">
                    <span class="flex items-center gap-2 font-semibold text-amber-400 text-sm">
                        <span class="h-2.5 w-2.5 rounded-full bg-amber-400 animate-ping"></span>
                        CONTRAT DERIV ACTIF #{act.get('contract_id')}
                    </span>
                    <span class="text-xs px-2.5 py-1 rounded bg-amber-500/20 text-amber-300 border border-amber-500/30 font-mono">
                        {act.get('contract_type')}
                    </span>
                </div>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm font-mono mt-3">
                    <div class="bg-slate-900/80 p-3 rounded-lg border border-slate-800">
                        <div class="text-xs text-slate-400">Direction</div>
                        <div class="font-bold {dir_cls}">{act['direction']}</div>
                    </div>
                    <div class="bg-slate-900/80 p-3 rounded-lg border border-slate-800">
                        <div class="text-xs text-slate-400">Prix Entrée</div>
                        <div class="font-bold text-amber-300">{act['entry_price']:.2f}$</div>
                    </div>
                    <div class="bg-slate-900/80 p-3 rounded-lg border border-slate-800">
                        <div class="text-xs text-slate-400">Cible TP</div>
                        <div class="font-bold text-indigo-400">{act['tp_price']:.2f}$</div>
                    </div>
                    <div class="bg-slate-900/80 p-3 rounded-lg border border-slate-800">
                        <div class="text-xs text-slate-400">Mise (Stake)</div>
                        <div class="font-bold text-emerald-400">{act['stake_usd']:.2f}$ (x{act['multiplier']})</div>
                    </div>
                </div>
            </div>
            """

        return f"""<!DOCTYPE html>
<html lang="fr" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cloud Bot 00h Gold — Portefeuille 100$ Deriv</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <meta http-equiv="refresh" content="10">
    <style>
        body {{ font-family: 'Inter', sans-serif; }}
        .font-mono {{ font-family: 'JetBrains Mono', monospace; }}
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen p-4 md:p-8">
    <div class="max-w-6xl mx-auto space-y-6">
        
        <!-- HEADER -->
        <header class="flex flex-col md:flex-row md:items-center justify-between pb-6 border-b border-slate-800 gap-4">
            <div>
                <div class="flex items-center gap-3">
                    <span class="h-3 w-3 rounded-full bg-amber-400 animate-pulse"></span>
                    <h1 class="text-2xl font-bold tracking-tight text-amber-300">BOT 00H GOLD (XAU/USD)</h1>
                    <span class="text-xs bg-amber-500/10 text-amber-400 border border-amber-500/30 px-2 py-0.5 rounded font-mono">Render Cloud</span>
                </div>
                <p class="text-xs text-slate-400 mt-1">Stratégie Minuit Fibo Miroir | Multiplicateurs Deriv x100 | Portefeuille 100$ Dédié</p>
            </div>
            <div class="flex items-center gap-4 bg-slate-900 border border-slate-800 px-4 py-2.5 rounded-xl font-mono text-sm">
                <div>
                    <span class="text-xs text-slate-400 block">Spot XAU/USD</span>
                    <span class="text-lg font-bold text-amber-300">{st['current_price']:.2f}$</span>
                </div>
                <div class="border-l border-slate-800 pl-4">
                    <span class="text-xs text-slate-400 block">Compte Deriv</span>
                    <span class="text-xs text-slate-200">{st['account_id']}</span>
                </div>
            </div>
        </header>

        <!-- CARTES KPI PORTEFEUILLE 100$ & IA -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div class="bg-slate-900/90 border border-slate-800 p-4 rounded-xl">
                <span class="text-xs text-slate-400 uppercase tracking-wider block mb-1">Portefeuille Dédié</span>
                <div class="text-2xl font-bold font-mono text-emerald-400">{st['portfolio_equity']:.2f} $</div>
                <span class="text-xs text-slate-400">Capital initial : {st['portfolio_capital_initial']:.2f} $</span>
            </div>
            <div class="bg-slate-900/90 border border-slate-800 p-4 rounded-xl">
                <span class="text-xs text-slate-400 uppercase tracking-wider block mb-1">Rendement Net (ROI)</span>
                <div class="text-2xl font-bold font-mono {'text-emerald-400' if st['portfolio_total_pnl'] >= 0 else 'text-rose-400'}">
                    {st['portfolio_total_pnl']:+.2f} $ ({st['portfolio_roi_pct']:+.1f}%)
                </div>
                <span class="text-xs text-slate-400">Sur {qb['total_trades']} sessions exécutées</span>
            </div>
            <div class="bg-slate-900/90 border border-slate-800 p-4 rounded-xl">
                <span class="text-xs text-slate-400 uppercase tracking-wider block mb-1">Taux de Réussite</span>
                <div class="text-2xl font-bold font-mono text-indigo-300">{qb['win_rate']:.1f}%</div>
                <span class="text-xs text-slate-400">Mise : {CFG['stake_usd']:.2f}$ (2% strict)</span>
            </div>
            <div class="bg-slate-900/90 border border-slate-800 p-4 rounded-xl">
                <span class="text-xs text-slate-400 uppercase tracking-wider block mb-1">Régime de Marché IA</span>
                <div class="text-lg font-bold font-mono text-amber-300">{qb['regime']}</div>
                <span class="text-xs text-slate-400 font-mono">ATR M15 : {qb['atr_m15']:.2f}$</span>
            </div>
        </div>

        <!-- STATUT & RÉFLEXION IA -->
        <div class="bg-slate-900/90 border border-indigo-500/30 p-5 rounded-xl space-y-3">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-2">
                    <span class="text-xs px-2.5 py-0.5 rounded-full bg-indigo-500/20 text-indigo-300 border border-indigo-500/40 font-mono">
                        QUANT BRAIN — CERVEAU AUTONOME
                    </span>
                    <span class="text-xs text-slate-400">{st['phase']}</span>
                </div>
                <span class="text-xs font-mono text-slate-400">Horloge UTC : {st['last_tick_utc']}</span>
            </div>
            <div class="text-sm text-slate-200 bg-slate-950/60 p-3 rounded-lg border border-slate-800">
                <span class="text-indigo-400 font-semibold text-xs uppercase block mb-1">Réflexion & Adaptation IA :</span>
                {qb['last_reflection']}
            </div>
            <div class="text-xs text-slate-400">
                Statut opérationnel : <span class="text-slate-200 font-mono">{st['phase_detail']}</span>
            </div>
        </div>

        <!-- CONTRAT ACTIF -->
        <div>
            <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Position en Cours (Deriv Multiplier)</h2>
            {active_card}
        </div>

        <!-- GRAPHIQUE M15 & BOUGIES -->
        <div class="bg-slate-900/90 border border-slate-800 p-5 rounded-xl">
            <div class="flex items-center justify-between mb-4">
                <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400">Graphique M15 — Gold (35 dernières bougies)</h2>
                <span class="text-xs text-amber-400 font-mono">Dernier tick : {st['current_price']:.2f}$</span>
            </div>
            <div class="w-full overflow-x-auto bg-slate-950/80 rounded-lg p-2 border border-slate-800/80 flex justify-center">
                <svg width="{svg_w}" height="{svg_h}" class="block">
                    {candles_svg}
                </svg>
            </div>
        </div>

        <!-- HISTORIQUE DES TRADES -->
        <div class="bg-slate-900/90 border border-slate-800 p-5 rounded-xl">
            <div class="flex items-center justify-between mb-4">
                <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400">Journal des Trades — XAU/USD Minuit</h2>
                <a href="/xauusd_cloud_journal.csv" class="text-xs text-amber-400 hover:underline font-mono">Export CSV</a>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left">
                    <thead>
                        <tr class="border-b border-slate-800 text-xs text-slate-400 uppercase tracking-wider font-mono">
                            <th class="py-2 px-3">#</th>
                            <th class="py-2 px-3">Session</th>
                            <th class="py-2 px-3">Ordre</th>
                            <th class="py-2 px-3">Entrée</th>
                            <th class="py-2 px-3">TP Cible</th>
                            <th class="py-2 px-3">SL Securité</th>
                            <th class="py-2 px-3">Sortie</th>
                            <th class="py-2 px-3">PnL USD</th>
                            <th class="py-2 px-3">Motif de Clôture</th>
                        </tr>
                    </thead>
                    <tbody>
                        {trade_rows if trade_rows else '<tr><td colspan="9" class="py-6 text-center text-slate-500 text-xs">Aucun trade enregistré pour l\'instant. Première session ce soir à 00h00 UTC.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- LOGS SYSTEME -->
        <div class="bg-slate-900/60 border border-slate-800/80 p-4 rounded-xl text-xs font-mono text-slate-400">
            <div class="text-slate-300 font-semibold mb-2">Derniers Événements Système :</div>
            <div class="space-y-1">
                {''.join(f'<div><span class="text-slate-500">[{l["timestamp_utc"]}]</span> <span class="text-amber-400">[{l["event_type"]}]</span> {l["message"]}</div>' for l in logs[:6])}
            </div>
        </div>

    </div>
</body>
</html>
"""


def run_http_server(port: int):
    server = HTTPServer(("0.0.0.0", port), XauusdDashboardHandler)
    log_event("SERVEUR_WEB", f"Dashboard Cloud XAU/USD démarré sur http://0.0.0.0:{port}")
    server.serve_forever()


# ==============================================================================
# BOUCLE PRINCIPALE AUTONOME
# ==============================================================================
def main():
    print("=" * 75, flush=True)
    print(" DÉMARRAGE DU BOT CLOUD DERIV — XAU/USD MINUIT 00H00 (RENDER)", flush=True)
    print("=" * 75, flush=True)
    init_db()
    fetch_deriv_account()
    recover_active_trade_on_startup()

    port = CFG["web_port"]
    t = threading.Thread(target=run_http_server, args=(port,), daemon=True)
    t.start()

    log_event("INITIALISATION", f"Bot Cloud XAU/USD prêt. Port={port} | Capital Alloué=100.00$ | Mise=2.00$")

    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            # Rafraîchir les bougies M15 toutes les 15 secondes
            try:
                fetch_m15_candles()
            except Exception as e:
                pass

            step_xau_strategy(now_dt)
        except Exception as e:
            log_event("BOUCLE_ERREUR", f"Exception dans la boucle principale: {e}")

        time.sleep(15)


if __name__ == "__main__":
    main()
