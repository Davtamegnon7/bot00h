#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Cloud Deriv — EUR/USD Session US (13h00 - 21h45 UTC)
Exécution exclusive sur l'API Deriv v1 (Options / Multiplicateurs)
Compte Démo : DOT95087458 (Token PAT + Deriv-App-ID)
Déployé sur Render.com : https://bot00h.onrender.com
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
    print("[ERREUR] Module websocket manquant. Installer avec: pip install websocket-client")
    sys.exit(1)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "eurusd_journal.db")
CSV_PATH = os.path.join(BASE_DIR, "eurusd_journal.csv")

CFG = {
    "web_port": int(os.environ.get("PORT", os.environ.get("BOT_PORT", "10000"))),
    "deriv_token": os.environ.get(
        "DERIV_API_TOKEN",
        "pat_4a3c8a93b232281da93a258af698fe850f2b2964eb00477ad05bf9d130ec11e0",
    ).strip(),
    "deriv_app_id": os.environ.get("DERIV_APP_ID", "34YCoOxGz6n07t9Gdmf66").strip(),
    "deriv_demo_account": "DOT95087458",
    "symbol": "frxEURUSD",
    "symbol_label": "EUR/USD",
    "stake_usd": float(os.environ.get("EUR_STAKE", "20.0")),
    "multiplier": int(os.environ.get("EUR_MULTIPLIER", "100")),
    "tolerance_pips": float(os.environ.get("EUR_TOLERANCE_PIPS", "1.8")),  # 1.8 pips = 0.00018
    "pip_size": 0.0001,
}

PUBLIC_WS_URLS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

STATE_LOCK = threading.Lock()
STATE = {
    "bot_name": "Cloud EUR/USD Bot (13h-21h45 UTC)",
    "environment": "Render Cloud Linux",
    "account_id": CFG["deriv_demo_account"],
    "account_balance": 10002.96,
    "account_currency": "USD",
    "symbol": CFG["symbol"],
    "current_price": 0.0,
    "last_tick_utc": "",
    "phase": "INITIALISATION",
    "phase_detail": "Démarrage du bot Cloud EUR/USD...",
    "session_date": "",
    "plan": None,
    "active_trade": None,
    "last_checked_utc": "",
    "candles_m15": [],
    "last_update_utc": "",
}


# ==============================================================================
# BASE DE DONNEES SQLITE & JOURNAL CSV
# ==============================================================================
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS eurusd_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_date TEXT,
            symbol TEXT,
            direction TEXT,
            contract_type TEXT,
            entry_price REAL,
            support_level REAL,
            tp1_level REAL,
            tp2_level REAL,
            stake_usd REAL,
            multiplier INTEGER,
            contract_id TEXT,
            entry_time_utc TEXT,
            exit_price REAL,
            exit_time_utc TEXT,
            exit_reason TEXT,
            pnl_usd REAL,
            status TEXT,
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


def log_event(event_type: str, message: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [EURUSD_CLOUD] [{event_type}] {message}", flush=True)
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO logs (timestamp_utc, event_type, message) VALUES (?, ?, ?)",
            (ts, event_type, message),
        )
        con.commit()
        con.close()
    except Exception:
        pass


def record_trade_open(tr: dict) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO eurusd_trades (
            session_date, symbol, direction, contract_type, entry_price,
            support_level, tp1_level, tp2_level, stake_usd, multiplier,
            contract_id, entry_time_utc, status, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tr["session_date"],
            tr["symbol"],
            tr["direction"],
            tr["contract_type"],
            tr["entry_price"],
            tr["support_level"],
            tr["tp1_level"],
            tr["tp2_level"],
            tr["stake_usd"],
            tr["multiplier"],
            tr["contract_id"],
            tr["entry_time_utc"],
            "OPEN",
            tr["notes"],
        ),
    )
    con.commit()
    tid = cur.lastrowid
    con.close()
    export_csv()
    return tid


def record_trade_close(trade_id: int, exit_price: float, exit_reason: str, pnl_usd: float, notes_append: str = ""):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE eurusd_trades
        SET exit_price = ?, exit_time_utc = ?, exit_reason = ?, pnl_usd = ?, status = 'CLOSED',
            notes = notes || ?
        WHERE id = ?
        """,
        (round(exit_price, 5), ts, exit_reason, round(pnl_usd, 2), " | " + notes_append if notes_append else "", trade_id),
    )
    con.commit()
    con.close()
    export_csv()


def export_csv():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM eurusd_trades ORDER BY id ASC")
        rows = cur.fetchall()
        con.close()
        if rows:
            with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                for r in rows:
                    writer.writerow(dict(r))
    except Exception as e:
        print(f"[CSV_EXPORT_ERROR] {e}")


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
        "User-Agent": "BotCloud-EURUSD",
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
                STATE["account_balance"] = float(demo_acc.get("balance", STATE["account_balance"]))
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
        "User-Agent": "BotCloud-EURUSD",
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


def fetch_m15_candles(symbol: str = CFG["symbol"], count: int = 80) -> list:
    for url in PUBLIC_WS_URLS:
        try:
            ws = websocket.create_connection(url, timeout=12)
            ws.send(json.dumps({
                "ticks_history": symbol,
                "style": "candles",
                "granularity": 900,
                "count": count,
                "end": "latest"
            }))
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
                    STATE["candles_m15"] = out[-35:]  # Conserver pour affichage graphique
                return out
        except Exception:
            continue
    raise RuntimeError(f"Flux WebSocket Deriv indisponible pour {symbol}")


def deriv_open_multiplier_contract(direction: str, entry_px: float, tp_px: float, sl_px: float = 0.0) -> tuple:
    """Ouvre un contrat multiplicateur MULTUP sur le compte Démo Deriv DOT95087458."""
    acc_id = STATE.get("account_id") or CFG["deriv_demo_account"]
    ws_url = get_deriv_ws_url(acc_id)
    if not ws_url:
        log_event("ORDRE_SIMULE", f"OTP Deriv indisponible, ordre simulé @ {entry_px:.5f}")
        return (f"SIMU-{int(time.time())}", entry_px)

    stake = CFG["stake_usd"]
    mult = CFG["multiplier"]
    ctype = "MULTUP" if direction.upper() == "BUY" else "MULTDOWN"

    # Calcul dynamique TP / SL en USD
    tp_distance = abs(tp_px - entry_px)
    tp_usd = max(0.50, round(stake * mult * (tp_distance / max(entry_px, 1.0)), 2))
    sl_usd = 15.0  # SL de sécurité par défaut à 15$

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
                "take_profit": min(tp_usd, 500.0),
                "stop_loss": min(sl_usd, 20.0),
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
                log_event("ORDRE_DERIV_REUSSI", f"Contrat {ctype} ouvert sur {acc_id} (ID={cid} | Stake={stake}$ | Mult={mult} | TP=+{tp_usd}$ | SL=-{sl_usd}$)")
                fetch_deriv_account()
                return (cid, entry_px)
            else:
                log_event("ACHAT_REFUSE", f"Reponse Deriv buy: {buy_res}")
        else:
            log_event("PROPOSAL_ERR", f"Reponse Deriv proposal: {prop_res}")
            ws.close()
    except Exception as e:
        log_event("ERREUR_ORDRE_DERIV", f"Exception ordre Deriv: {e}")

    return (f"DERIV-DEMO-{int(time.time())}", entry_px)


def check_deriv_active_contract(contract_id: str) -> dict:
    """Verifie le statut en cours du contrat via Deriv API."""
    acc_id = STATE.get("account_id") or CFG["deriv_demo_account"]
    ws_url = get_deriv_ws_url(acc_id)
    if not ws_url:
        return {}
    try:
        ws = websocket.create_connection(ws_url, timeout=12)
        ws.send(json.dumps({"portfolio": 1}))
        res = json.loads(ws.recv())
        ws.close()
        contracts = res.get("portfolio", {}).get("contracts", [])
        for c in contracts:
            if str(c.get("contract_id")) == str(contract_id):
                return c
        return {"status": "CLOSED"}
    except Exception:
        return {}


# ==============================================================================
# LOGIQUE STRATÉGIE EUR/USD (SESSION US 13H00 - 21H45 UTC)
# ==============================================================================
def analyze_eur_us_session(candles: list, session_date: str) -> dict:
    """
    Analyse les 35 bougies M15 de la session US (13h00 a 21h30 UTC).
    Calcule :
      - Support = point le plus bas de la baisse
      - TP1 = point le plus haut du range qui suit
      - TP2 = TP1 + (TP1 - Support)
      - Tolerance de retouche = 1.8 pips (0.00018)
    """
    session_bars = []
    for c in candles:
        dt = datetime.fromtimestamp(c["epoch"], timezone.utc)
        d_str = dt.strftime("%Y-%m-%d")
        hhmm = dt.strftime("%H:%M")
        if d_str == session_date and "13:00" <= hhmm <= "21:30":
            session_bars.append((hhmm, c))

    if len(session_bars) < 25:
        return {"valid": False, "reason": f"Seulement {len(session_bars)}/35 bougies US disponibles"}

    # 1. Point le plus bas (Support de la baisse)
    lowest_low = min(b["low"] for _, b in session_bars)
    low_idx = next(i for i, (_, b) in enumerate(session_bars) if b["low"] == lowest_low)
    low_time = session_bars[low_idx][0]

    # 2. Point le plus haut du range (apres le plus bas)
    subsequent = session_bars[low_idx:]
    if len(subsequent) >= 2:
        highest_high = max(b["high"] for _, b in subsequent)
        high_idx = next(i for i, (_, b) in enumerate(session_bars) if b["high"] == highest_high)
        high_time = session_bars[high_idx][0]
    else:
        highest_high = max(b["high"] for _, b in session_bars)
        high_idx = next(i for i, (_, b) in enumerate(session_bars) if b["high"] == highest_high)
        high_time = session_bars[high_idx][0]

    height = highest_high - lowest_low
    height_pips = height / CFG["pip_size"]

    if height_pips < 5.0:
        return {"valid": False, "reason": f"Amplitude US trop faible ({height_pips:.1f} pips)"}

    is_bear_session = (high_idx < low_idx)
    session_type = "BAISSE" if is_bear_session else "HAUSSE"
    tolerance = CFG["tolerance_pips"] * CFG["pip_size"]

    if session_type == "BAISSE":
        trigger_level = lowest_low
        level_name = "Support"
        trade_dir = "BUY"
        ctype = "MULTUP"
        tp1 = highest_high
        tp2 = tp1 + height
    else:
        trigger_level = highest_high
        level_name = "Résistance"
        trade_dir = "SELL"
        ctype = "MULTDOWN"
        tp1 = lowest_low
        tp2 = tp1 - height

    return {
        "valid": True,
        "session_date": session_date,
        "session_type": session_type,
        "trade_direction": trade_dir,
        "contract_type": ctype,
        "trigger_level": round(trigger_level, 5),
        "level_name": level_name,
        "support": round(lowest_low, 5),
        "resistance": round(highest_high, 5),
        "support_time": low_time,
        "tp1": round(tp1, 5),
        "tp1_time": high_time,
        "tp2": round(tp2, 5),
        "height_pips": round(height_pips, 1),
        "tolerance": tolerance,
        "tolerance_pips": CFG["tolerance_pips"],
        "tp1_is_support": False,
        "bars_count": len(session_bars),
    }


def step_eurusd_bot(now_dt: datetime):
    today_str = now_dt.strftime("%Y-%m-%d")
    hhmm = now_dt.strftime("%H:%M")

    # Maintien du solde et compte Deriv
    if STATE["account_balance"] == 0 or not STATE.get("account_id"):
        fetch_deriv_account()

    # 1. Recuperation des bougies M15
    try:
        candles = fetch_m15_candles(CFG["symbol"], count=80)
    except Exception as e:
        with STATE_LOCK:
            STATE["phase_detail"] = f"Connexion flux bougies : {e}"
        return

    cur_px = candles[-1]["close"]

    # 2. Gestion de la phase horaire
    with STATE_LOCK:
        active_tr = STATE["active_trade"]
        plan = STATE["plan"]

    # A) Avant 13h00 UTC : En attente
    if hhmm < "13:00":
        with STATE_LOCK:
            STATE["phase"] = "ATTENTE_SESSION_US"
            STATE["phase_detail"] = f"En attente de l'ouverture de la session US à 13h00 UTC (Heure actuelle: {hhmm} UTC)"
            STATE["session_date"] = today_str
        return

    # B) Entre 13h00 UTC et 21h45 UTC : Observation de la session US
    if "13:00" <= hhmm < "21:45":
        with STATE_LOCK:
            STATE["phase"] = "OBSERVATION_SESSION_US"
            STATE["phase_detail"] = f"Observation de la session US en cours (13h00 - 21h45 UTC). Prix spot: {cur_px:.5f}"
            STATE["session_date"] = today_str
        return

    # C) A 21h45 UTC (ou apres) : Calcul et traçage officiel des niveaux
    if hhmm >= "21:45" and (plan is None or plan.get("session_date") != today_str):
        analysis = analyze_eur_us_session(candles, today_str)
        if analysis["valid"]:
            with STATE_LOCK:
                STATE["plan"] = analysis
                STATE["phase"] = "SURVEILLANCE_ACTIVE"
                STATE["phase_detail"] = (
                    f"Niveaux 21h45 tracés : Support={analysis['support']:.5f} | "
                    f"TP1={analysis['tp1']:.5f} | TP2={analysis['tp2']:.5f} | "
                    f"Tolérance={analysis['tolerance_pips']} pips"
                )
            log_event(
                "NIVEAUX_TRACES_21H45",
                f"Session US {today_str} validée : Support={analysis['support']:.5f} (à {analysis['support_time']}), "
                f"TP1={analysis['tp1']:.5f} (à {analysis['tp1_time']}), TP2={analysis['tp2']:.5f} (+{analysis['height_pips']} pips). "
                f"Surveillance des retouches activée sans limite de temps.",
            )
        else:
            with STATE_LOCK:
                STATE["phase"] = "ATTENTE_DONNEES"
                STATE["phase_detail"] = f"Analyse 21h45 : {analysis.get('reason')}"
            return

    # D) Surveillance continue apres 21h45 UTC (si aucun trade n'est en cours)
    with STATE_LOCK:
        plan = STATE["plan"]
        active_tr = STATE["active_trade"]

    if plan and plan.get("session_date") == today_str and active_tr is None:
        last_bar = candles[-1]
        support = plan["support"]
        tp1 = plan["tp1"]
        tp2 = plan["tp2"]
        tol = plan["tolerance"]
        session_type = plan.get("session_type", "BAISSE")

        # Cas 1 : Session de Baisse -> BUY sur retouche du Support
        if session_type == "BAISSE":
            if not plan.get("tp1_is_support", False) and last_bar["high"] >= tp1:
                plan["tp1_is_support"] = True
                plan["support"] = tp1
                plan["support_time"] = "Cassure TP1"
                with STATE_LOCK:
                    STATE["plan"] = plan
                    STATE["phase_detail"] = f"TP1 atteint en premier -> TP1 ({tp1:.5f}) devient le nouveau Support ! Cible = TP2 ({tp2:.5f})"
                log_event(
                    "TP1_DEVIENT_SUPPORT",
                    f"Le prix a touché TP1 ({tp1:.5f}) sans avoir retouché le Support initial. TP1 devient le nouveau Support, objectif TP2 ({tp2:.5f}).",
                )
                return

            target_tp = tp2 if plan.get("tp1_is_support") else tp1
            zone_high = support + tol
            if last_bar["low"] <= zone_high and last_bar["close"] >= (support - 0.00030):
                gap_pips = (last_bar["low"] - support) / CFG["pip_size"]
                note = (
                    f"Retouche du Support ({support:.5f}) après 21h45 par la bougie "
                    f"(bas={last_bar['low']:.5f}, écart={gap_pips:.1f} pips <= {plan['tolerance_pips']} pips) "
                    f"-> BUY MULTUP avec objectif {target_tp:.5f}"
                )
                execute_eur_order("BUY", plan, last_bar, target_tp, note)
                return

        # Cas 2 : Session de Hausse -> SELL sur retouche de la Résistance
        elif session_type == "HAUSSE":
            resistance = plan["resistance"]
            tp1_sell = plan["tp1"]
            tp2_sell = plan["tp2"]

            if not plan.get("tp1_is_resistance", False) and last_bar["low"] <= tp1_sell:
                plan["tp1_is_resistance"] = True
                plan["resistance"] = tp1_sell
                with STATE_LOCK:
                    STATE["plan"] = plan
                    STATE["phase_detail"] = f"TP1 atteint en premier -> TP1 ({tp1_sell:.5f}) devient la nouvelle Résistance ! Cible = TP2 ({tp2_sell:.5f})"
                log_event(
                    "TP1_DEVIENT_RESISTANCE",
                    f"Le prix a touché TP1 ({tp1_sell:.5f}) sans avoir retouché la Résistance. Cible = TP2 ({tp2_sell:.5f}).",
                )
                return

            target_tp = tp2_sell if plan.get("tp1_is_resistance") else tp1_sell
            zone_low = resistance - tol
            if last_bar["high"] >= zone_low and last_bar["close"] <= (resistance + 0.00030):
                gap_pips = (resistance - last_bar["high"]) / CFG["pip_size"]
                note = (
                    f"Retouche de la Résistance ({resistance:.5f}) après 21h45 par la bougie "
                    f"(haut={last_bar['high']:.5f}, écart={gap_pips:.1f} pips <= {plan['tolerance_pips']} pips) "
                    f"-> SELL MULTDOWN avec objectif {target_tp:.5f}"
                )
                execute_eur_order("SELL", plan, last_bar, target_tp, note)
                return

    # E) Suivi d'un trade actif
    if active_tr is not None:
        manage_active_trade(candles)


def execute_eur_order(direction: str, plan: dict, bar: dict, target_tp: float, reason_note: str):
    entry_px = bar["close"]
    ctype = "MULTUP" if direction.upper() == "BUY" else "MULTDOWN"
    sl_px = (plan["support"] - 0.0010) if direction == "BUY" else (plan.get("resistance", entry_px + 0.0010) + 0.0010)
    cid, exec_px = deriv_open_multiplier_contract(
        direction,
        entry_px,
        target_tp,
        sl_px=sl_px,
    )

    tr = {
        "session_date": plan["session_date"],
        "symbol": CFG["symbol"],
        "direction": direction,
        "contract_type": ctype,
        "entry_price": round(exec_px, 5),
        "support_level": plan.get("support", 0.0),
        "tp1_level": plan["tp1"],
        "tp2_level": plan["tp2"],
        "stake_usd": CFG["stake_usd"],
        "multiplier": CFG["multiplier"],
        "contract_id": cid,
        "entry_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "notes": reason_note,
    }
    tid = record_trade_open(tr)
    tr["id"] = tid

    with STATE_LOCK:
        STATE["active_trade"] = tr
        STATE["phase"] = "TRADE_ACTIF"
        STATE["phase_detail"] = f"Ordre {direction} {ctype} ouvert @ {exec_px:.5f} (Cible: {target_tp:.5f} | Contrat #{cid})"

    log_event("ORDRE_DECLENCHE", reason_note)


def manage_active_trade(candles: list):
    with STATE_LOCK:
        tr = STATE["active_trade"]
        plan = STATE["plan"]

    if not tr:
        return

    cur_px = candles[-1]["close"]
    target_tp = plan["tp2"] if plan.get("tp1_is_support") or plan.get("tp1_is_resistance") else plan["tp1"]

    # 1. Vérification si TP atteint
    if tr["direction"] == "BUY" and cur_px >= target_tp:
        pnl_est = round(tr["stake_usd"] * tr["multiplier"] * ((cur_px - tr["entry_price"]) / tr["entry_price"]), 2)
        record_trade_close(tr["id"], cur_px, "Take Profit Atteint", pnl_est, f"Objectif BUY {target_tp:.5f} atteint !")
        with STATE_LOCK:
            STATE["active_trade"] = None
            STATE["phase"] = "CLOTURE_REUSSIE"
            STATE["phase_detail"] = f"Trade BUY terminé avec gain estimé: +{pnl_est:.2f} $"
        log_event("TRADE_FERME", f"Take Profit BUY atteint à {cur_px:.5f} | PnL: +{pnl_est:.2f} $")
    elif tr["direction"] == "SELL" and cur_px <= target_tp:
        pnl_est = round(tr["stake_usd"] * tr["multiplier"] * ((tr["entry_price"] - cur_px) / tr["entry_price"]), 2)
        record_trade_close(tr["id"], cur_px, "Take Profit Atteint", pnl_est, f"Objectif SELL {target_tp:.5f} atteint !")
        with STATE_LOCK:
            STATE["active_trade"] = None
            STATE["phase"] = "CLOTURE_REUSSIE"
            STATE["phase_detail"] = f"Trade SELL terminé avec gain estimé: +{pnl_est:.2f} $"
        log_event("TRADE_FERME", f"Take Profit SELL atteint à {cur_px:.5f} | PnL: +{pnl_est:.2f} $")
        log_event("TAKE_PROFIT_ATTEINT", f"Contrat #{tr['contract_id']} clôturé au TP ({cur_px:.5f}) | Gain estimé: +{pnl_est}$")
        with STATE_LOCK:
            STATE["active_trade"] = None
            STATE["phase"] = "SURVEILLANCE_TERMINEE"
            STATE["phase_detail"] = f"Trade gagnant clôturé au TP ({target_tp:.5f}) | Gain: +{pnl_est}$"
        fetch_deriv_account()
        return

    # 2. Vérification du statut du contrat Deriv via WebSocket API
    if tr.get("contract_id") and not str(tr["contract_id"]).startswith("SIMU"):
        info = check_deriv_active_contract(tr["contract_id"])
        if info.get("status") == "CLOSED":
            # Le contrat s'est fermé sur TP ou SL sur Deriv
            pnl_real = float(info.get("profit", 0.0))
            record_trade_close(tr["id"], cur_px, "Clôture Deriv Directe", pnl_real, "Fermé sur Deriv")
            log_event("CLOTURE_DERIV", f"Contrat #{tr['contract_id']} fermé sur Deriv. PnL: {pnl_real}$")
            with STATE_LOCK:
                STATE["active_trade"] = None
                STATE["phase"] = "SURVEILLANCE_ACTIVE"
                STATE["phase_detail"] = f"Dernier contrat fermé sur Deriv (PnL: {pnl_real:+.2f}$)"
            fetch_deriv_account()


# ==============================================================================
# BOUCLE PRINCIPALE & KEEP-ALIVE RENDER
# ==============================================================================
def bot_loop():
    log_event("DEMARRAGE", f"Bot Cloud EUR/USD actif sur Deriv Demo {CFG['deriv_demo_account']}")
    fetch_deriv_account()

    last_ping = 0.0
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            step_eurusd_bot(now_utc)

            # Keep-alive anti-veille Render.com toutes les 3 minutes
            if time.time() - last_ping > 180.0:
                last_ping = time.time()
                render_url = os.environ.get("RENDER_EXTERNAL_URL", "https://bot00h.onrender.com").rstrip("/")
                if render_url:
                    try:
                        req = urllib.request.Request(f"{render_url}/api/status", headers={"User-Agent": "Render-KeepAlive"})
                        urllib.request.urlopen(req, timeout=8).read()
                    except Exception:
                        pass
        except Exception as e:
            log_event("ERREUR_BOUCLE", f"Exception non bloquante: {e}")

        time.sleep(10)


# ==============================================================================
# DASHBOARD WEB RENDER (HTML5 / DARK MODERN UI)
# ==============================================================================
def render_dashboard_html() -> str:
    init_db()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM eurusd_trades ORDER BY id DESC LIMIT 50")
    trades = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 30")
    logs = [dict(r) for r in cur.fetchall()]
    con.close()

    with STATE_LOCK:
        s = dict(STATE)
        plan = s.get("plan")
        trade = s.get("active_trade")
        candles = list(s.get("candles_m15", []))

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Génération du graphique SVG M15
    svg_chart = render_svg_chart(candles, plan, s.get("current_price", 0.0))

    plan_html = ""
    if plan and plan.get("valid"):
        plan_html = f"""
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
            <div class="card-metric border-l-4 border-emerald-500">
                <div class="text-xs text-gray-400">SUPPORT (PLUS BAS)</div>
                <div class="text-xl font-bold text-emerald-400 font-mono">{plan['support']:.5f}</div>
                <div class="text-xs text-gray-500">Heure: {plan['support_time']} UTC</div>
            </div>
            <div class="card-metric border-l-4 border-cyan-500">
                <div class="text-xs text-gray-400">TP 1 (HAUT RANGE)</div>
                <div class="text-xl font-bold text-cyan-400 font-mono">{plan['tp1']:.5f}</div>
                <div class="text-xs text-gray-500">Heure: {plan['tp1_time']} UTC</div>
            </div>
            <div class="card-metric border-l-4 border-purple-500">
                <div class="text-xs text-gray-400">TP 2 (PROJECTION HAUTEUR)</div>
                <div class="text-xl font-bold text-purple-400 font-mono">{plan['tp2']:.5f}</div>
                <div class="text-xs text-gray-500">Hauteur: +{plan['height_pips']} pips</div>
            </div>
            <div class="card-metric border-l-4 border-amber-500">
                <div class="text-xs text-gray-400">ZONE TOLERANCE</div>
                <div class="text-xl font-bold text-amber-400 font-mono">+{plan['tolerance_pips']} pips</div>
                <div class="text-xs text-gray-500">Seuil: {(plan['support'] + plan['tolerance']):.5f}</div>
            </div>
        </div>
        """
    else:
        plan_html = f"""
        <div class="p-4 bg-gray-900/60 rounded-xl border border-gray-800 text-sm text-gray-400">
            ⏳ <strong>En attente de 21h45 UTC :</strong> Les niveaux Support, TP1 et TP2 seront calculés et verrouillés précisément à la clôture de la session US (21h45 UTC).
        </div>
        """

    trade_status_html = ""
    if trade:
        trade_status_html = f"""
        <div class="p-4 bg-emerald-950/40 border border-emerald-600 rounded-xl mb-6">
            <div class="flex items-center justify-between mb-2">
                <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-emerald-500 text-black">
                    ● CONTRAT ACTIF #{trade.get('contract_id', 'SIMU')}
                </span>
                <span class="text-xs text-emerald-300 font-mono">Entrée: {trade['entry_time_utc']}</span>
            </div>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                <div><span class="text-gray-400">Direction:</span> <strong class="text-emerald-400">MULTUP (BUY)</strong></div>
                <div><span class="text-gray-400">Prix Entrée:</span> <span class="font-mono font-bold text-white">{trade['entry_price']:.5f}</span></div>
                <div><span class="text-gray-400">Cible TP:</span> <span class="font-mono font-bold text-cyan-400">{trade.get('tp1_level'):.5f}</span></div>
                <div><span class="text-gray-400">Stake / Mult:</span> <span class="font-mono text-white">{trade['stake_usd']}$ (x{trade['multiplier']})</span></div>
            </div>
        </div>
        """

    trades_rows = ""
    for t in trades:
        pnl = t.get("pnl_usd")
        pnl_badge = '<span class="text-gray-400">-</span>'
        if pnl is not None:
            col = "text-emerald-400 font-bold" if pnl >= 0 else "text-rose-400 font-bold"
            pnl_badge = f'<span class="{col}">{pnl:+.2f} $</span>'

        trades_rows += f"""
        <tr class="border-b border-gray-800 hover:bg-gray-800/40 text-xs font-mono">
            <td class="py-2.5 px-3">{t['id']}</td>
            <td class="py-2.5 px-3 text-gray-300">{t['session_date']}</td>
            <td class="py-2.5 px-3"><span class="px-2 py-0.5 rounded bg-emerald-900/60 text-emerald-300 font-bold">{t['direction']}</span></td>
            <td class="py-2.5 px-3">{t['entry_price']:.5f}</td>
            <td class="py-2.5 px-3 text-emerald-400">{t['support_level']:.5f}</td>
            <td class="py-2.5 px-3 text-cyan-400">{t['tp1_level']:.5f}</td>
            <td class="py-2.5 px-3 text-purple-400">{t['tp2_level']:.5f}</td>
            <td class="py-2.5 px-3 text-gray-300">{t.get('exit_price') or '-'}</td>
            <td class="py-2.5 px-3">{pnl_badge}</td>
            <td class="py-2.5 px-3 text-gray-400">{t.get('exit_reason') or 'En cours'}</td>
        </tr>
        """

    logs_rows = ""
    for l in logs:
        logs_rows += f"""
        <div class="text-xs font-mono py-1 border-b border-gray-800/50 flex space-x-2">
            <span class="text-gray-500 whitespace-nowrap">{l['timestamp_utc']}</span>
            <span class="text-cyan-400 font-semibold whitespace-nowrap">[{l['event_type']}]</span>
            <span class="text-gray-300">{l['message']}</span>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="fr" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bot Cloud Deriv — EUR/USD Session US (13h00 - 21h45)</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <meta http-equiv="refresh" content="5">
    <style>
        body {{ background-color: #0b0f17; color: #e2e8f0; font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; }}
        .card-metric {{ background: rgba(17, 24, 39, 0.7); backdrop-filter: blur(8px); padding: 1rem; border-radius: 0.75rem; border: 1px solid rgba(255, 255, 255, 0.07); }}
    </style>
</head>
<body class="p-4 md:p-8 max-w-7xl mx-auto">
    <!-- HEADER -->
    <header class="flex flex-col md:flex-row md:items-center justify-between pb-6 border-b border-gray-800 gap-4">
        <div>
            <div class="flex items-center space-x-3">
                <span class="h-3 w-3 rounded-full bg-emerald-500 animate-pulse"></span>
                <h1 class="text-xl md:text-2xl font-black tracking-tight text-white">BOT CLOUD DERIV — EUR/USD (SESSION US)</h1>
            </div>
            <p class="text-xs text-gray-400 mt-1">Stratégie 13h00 - 21h45 UTC (Support Baisse, TP1 Haut Range, TP2, Retouche Support) | Hébergé sur Render</p>
        </div>
        <div class="flex flex-wrap items-center gap-3">
            <div class="px-3 py-1.5 rounded-lg bg-gray-900 border border-gray-800 text-xs">
                <span class="text-gray-400">Compte Démo :</span> <strong class="text-emerald-400 font-mono">{s['account_id']}</strong>
            </div>
            <div class="px-3 py-1.5 rounded-lg bg-gray-900 border border-gray-800 text-xs">
                <span class="text-gray-400">Solde :</span> <strong class="text-white font-mono">{s['account_balance']:.2f} {s['account_currency']}</strong>
            </div>
            <a href="/export/csv" class="px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold">Télécharger CSV</a>
        </div>
    </header>

    <!-- STATUS CARD -->
    <div class="mt-6 mb-6 p-4 rounded-xl bg-gradient-to-r from-blue-950/40 via-purple-950/30 to-gray-900/50 border border-blue-800/40 flex flex-col md:flex-row md:items-center justify-between gap-3">
        <div>
            <div class="text-xs font-bold text-blue-400 uppercase tracking-wider">STATUT DE LA SESSION ACTUELLE</div>
            <div class="text-lg font-bold text-white mt-0.5">{s['phase']}</div>
            <div class="text-xs text-gray-300 mt-0.5">{s['phase_detail']}</div>
        </div>
        <div class="text-right">
            <div class="text-xs text-gray-400">PRIX SPOT EUR/USD</div>
            <div class="text-2xl font-black font-mono text-amber-400">{s['current_price']:.5f}</div>
            <div class="text-[10px] text-gray-500">{now_utc}</div>
        </div>
    </div>

    <!-- TRADE ACTIF (SI PRESENT) -->
    {trade_status_html}

    <!-- NIVEAUX TRACÉS -->
    <section class="mb-6">
        <h2 class="text-sm font-bold text-gray-300 uppercase tracking-wider mb-2">NIVEAUX DE LA SESSION US (21h45 UTC)</h2>
        {plan_html}
    </section>

    <!-- GRAPHIQUE M15 & TRACÉS -->
    <section class="mb-6 card-metric">
        <div class="flex items-center justify-between mb-3">
            <h2 class="text-sm font-bold text-gray-300 uppercase tracking-wider">GRAPHIQUE EUR/USD M15 & NIVEAUX TRACÉS</h2>
            <div class="flex items-center space-x-3 text-xs">
                <span class="inline-flex items-center"><span class="w-3 h-0.5 bg-emerald-400 mr-1.5"></span> Support</span>
                <span class="inline-flex items-center"><span class="w-3 h-0.5 bg-cyan-400 mr-1.5"></span> TP1</span>
                <span class="inline-flex items-center"><span class="w-3 h-0.5 bg-purple-400 mr-1.5"></span> TP2</span>
                <span class="inline-flex items-center"><span class="w-3 h-0.5 bg-amber-400 mr-1.5"></span> Prix Spot</span>
            </div>
        </div>
        <div class="w-full overflow-x-auto">
            {svg_chart}
        </div>
    </section>

    <!-- HISTORIQUE DES TRADES -->
    <section class="mb-6 card-metric">
        <h2 class="text-sm font-bold text-gray-300 uppercase tracking-wider mb-3">HISTORIQUE DES TRADES EUR/USD</h2>
        <div class="overflow-x-auto">
            <table class="w-full text-left">
                <thead>
                    <tr class="text-[11px] text-gray-400 uppercase border-b border-gray-800">
                        <th class="py-2 px-3">#</th>
                        <th class="py-2 px-3">Date</th>
                        <th class="py-2 px-3">Ordre</th>
                        <th class="py-2 px-3">Entrée</th>
                        <th class="py-2 px-3">Support</th>
                        <th class="py-2 px-3">TP1</th>
                        <th class="py-2 px-3">TP2</th>
                        <th class="py-2 px-3">Sortie</th>
                        <th class="py-2 px-3">PnL</th>
                        <th class="py-2 px-3">Motif</th>
                    </tr>
                </thead>
                <tbody>
                    {trades_rows if trades_rows else '<tr><td colspan="10" class="py-4 text-center text-xs text-gray-500">Aucun trade enregistré pour le moment.</td></tr>'}
                </tbody>
            </table>
        </div>
    </section>

    <!-- LOGS CONSOLE -->
    <section class="card-metric">
        <h2 class="text-sm font-bold text-gray-300 uppercase tracking-wider mb-3">JOURNAL DES ÉVÉNEMENTS (LOGS DERIV)</h2>
        <div class="max-h-60 overflow-y-auto space-y-1 pr-2">
            {logs_rows if logs_rows else '<div class="text-xs text-gray-500">En attente de logs...</div>'}
        </div>
    </section>

    <footer class="mt-8 text-center text-[11px] text-gray-600">
        Bot Cloud EUR/USD Démo Deriv • Instance Render.com • Actualisation auto toutes les 5 secondes
    </footer>
</body>
</html>
"""
    return html


def render_svg_chart(candles: list, plan: dict, cur_px: float) -> str:
    """Génère un graphique SVG vectoriel ultra-léger représentant les bougies M15 et les niveaux tracés."""
    if not candles or len(candles) < 5:
        return '<div class="p-8 text-center text-xs text-gray-500">Données de bougies insuffisantes pour tracer le graphique.</div>'

    width = 960
    height = 280
    pad_top = 25
    pad_bottom = 25
    pad_left = 65
    pad_right = 65

    all_highs = [c["high"] for c in candles]
    all_lows = [c["low"] for c in candles]

    if plan and plan.get("valid"):
        all_highs.append(plan["tp2"])
        all_lows.append(plan["support"])
    if cur_px > 0:
        all_highs.append(cur_px)
        all_lows.append(cur_px)

    max_val = max(all_highs)
    min_val = min(all_lows)
    val_range = max(max_val - min_val, 0.0005)

    def y_pos(val: float) -> float:
        norm = (val - min_val) / val_range
        return height - pad_bottom - norm * (height - pad_top - pad_bottom)

    n = len(candles)
    chart_w = width - pad_left - pad_right
    bar_spacing = chart_w / max(n, 1)
    bar_width = max(bar_spacing * 0.65, 3.0)

    svg_elements = []

    # Lignes horizontales de référence des prix
    for steps in range(5):
        v = min_val + (steps / 4.0) * val_range
        yp = y_pos(v)
        svg_elements.append(f'<line x1="{pad_left}" y1="{yp}" x2="{width - pad_right}" y2="{yp}" stroke="#1e293b" stroke-dasharray="2,4" />')
        svg_elements.append(f'<text x="{pad_left - 8}" y="{yp + 3}" fill="#64748b" font-size="9" text-anchor="end" font-family="monospace">{v:.5f}</text>')

    # Bougies M15
    for i, c in enumerate(candles):
        x_center = pad_left + i * bar_spacing + (bar_spacing / 2.0)
        y_open = y_pos(c["open"])
        y_close = y_pos(c["close"])
        y_high = y_pos(c["high"])
        y_low = y_pos(c["low"])

        is_bullish = c["close"] >= c["open"]
        candle_col = "#10b981" if is_bullish else "#f43f5e"

        # Mèche
        svg_elements.append(f'<line x1="{x_center}" y1="{y_high}" x2="{x_center}" y2="{y_low}" stroke="{candle_col}" stroke-width="1.2" />')

        # Corps
        body_top = min(y_open, y_close)
        body_h = max(abs(y_close - y_open), 1.5)
        body_x = x_center - (bar_width / 2.0)
        svg_elements.append(f'<rect x="{body_x}" y="{body_top}" width="{bar_width}" height="{body_h}" fill="{candle_col}" rx="0.5" />')

    # Tracés des niveaux clés (si plan calculé)
    if plan and plan.get("valid"):
        sup_y = y_pos(plan["support"])
        tp1_y = y_pos(plan["tp1"])
        tp2_y = y_pos(plan["tp2"])
        tol_y = y_pos(plan["support"] + plan["tolerance"])

        # Zone de tolérance (rectangle transparent)
        z_top = min(sup_y, tol_y)
        z_h = abs(sup_y - tol_y)
        svg_elements.append(f'<rect x="{pad_left}" y="{z_top}" width="{chart_w}" height="{z_h}" fill="#10b981" opacity="0.08" />')

        # Ligne Support (Vert)
        svg_elements.append(f'<line x1="{pad_left}" y1="{sup_y}" x2="{width - pad_right}" y2="{sup_y}" stroke="#10b981" stroke-width="2" stroke-dasharray="4,3" />')
        svg_elements.append(f'<text x="{width - pad_right + 6}" y="{sup_y + 3}" fill="#10b981" font-size="10" font-weight="bold" font-family="monospace">SUP {plan["support"]:.5f}</text>')

        # Ligne TP1 (Cyan)
        svg_elements.append(f'<line x1="{pad_left}" y1="{tp1_y}" x2="{width - pad_right}" y2="{tp1_y}" stroke="#06b6d4" stroke-width="2" stroke-dasharray="4,3" />')
        svg_elements.append(f'<text x="{width - pad_right + 6}" y="{tp1_y + 3}" fill="#06b6d4" font-size="10" font-weight="bold" font-family="monospace">TP1 {plan["tp1"]:.5f}</text>')

        # Ligne TP2 (Violet)
        svg_elements.append(f'<line x1="{pad_left}" y1="{tp2_y}" x2="{width - pad_right}" y2="{tp2_y}" stroke="#a855f7" stroke-width="2" stroke-dasharray="4,3" />')
        svg_elements.append(f'<text x="{width - pad_right + 6}" y="{tp2_y + 3}" fill="#a855f7" font-size="10" font-weight="bold" font-family="monospace">TP2 {plan["tp2"]:.5f}</text>')

    # Prix Spot en direct (Jaune)
    if cur_px > 0:
        cur_y = y_pos(cur_px)
        svg_elements.append(f'<line x1="{pad_left}" y1="{cur_y}" x2="{width - pad_right}" y2="{cur_y}" stroke="#facc15" stroke-width="1.5" stroke-dasharray="2,2" />')
        svg_elements.append(f'<text x="{width - pad_right + 6}" y="{cur_y + 3}" fill="#facc15" font-size="10" font-weight="bold" font-family="monospace">{cur_px:.5f}</text>')

    svg_content = "\n".join(svg_elements)
    return f"""
    <svg viewBox="0 0 {width} {height}" class="w-full h-auto bg-[#070b12] rounded-lg border border-gray-800" style="min-width: 600px;">
        {svg_content}
    </svg>
    """


# ==============================================================================
# SERVEUR HTTP API & DASHBOARD
# ==============================================================================
class CloudBotHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/dashboard":
            body = render_dashboard_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/api/status":
            with STATE_LOCK:
                st = dict(STATE)
                st.pop("candles_m15", None)
            body = json.dumps(st, indent=2, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/export/csv":
            if os.path.exists(CSV_PATH):
                with open(CSV_PATH, "rb") as f:
                    content = f.read()
            else:
                content = b"id,session_date,symbol,direction,entry_price\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="eurusd_journal.csv"')
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Bot Cloud Deriv — EUR/USD (13h00 - 21h45 UTC)")
    parser.add_argument("--port", type=int, default=CFG["web_port"], help="Port HTTP")
    args = parser.parse_args()
    CFG["web_port"] = args.port

    init_db()

    # Démarrage de la boucle du bot dans un thread
    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()

    srv = HTTPServer(("0.0.0.0", CFG["web_port"]), CloudBotHTTPHandler)
    log_event("SERVEUR_WEB", f"Dashboard Cloud EUR/USD actif sur port {CFG['web_port']}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
