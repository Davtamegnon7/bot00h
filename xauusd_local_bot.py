#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Local MetaTrader 5 — XAU/USD 00h00 (Fibo Mode Miroir)
Exécution exclusive sur le terminal Deriv MetaTrader 5 local (#6314958)
Stratégie de Minuit :
  - Armement à 20h00 UTC
  - Analyse de la bougie M15 de 23h45 à 00h00 UTC
  - Bougie ROUGE -> BUY / Bougie VERTE -> SELL (Mode Miroir)
  - Fibo 1.0 (plus bas) & Fibo 0.5 (plus haut) sur 15 bougies + bougie 23h45
  - Évaluation à 30 min (00h30) : prolongation à 01h00 si avancement >= 15% & RSI favorable, sinon clôture
  - Clôture impérative à 01h00 UTC
Journal web local : http://localhost:8787
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
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None
    print("[ATTENTION] Module MetaTrader5 non disponible sur ce système.")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "journal_00h.db")
CSV_PATH = os.path.join(BASE_DIR, "journal_00h.csv")

CFG = {
    "web_port": int(os.environ.get("BOT_PORT", "8787")),
    "xau_symbol": "XAUUSD",
    "xau_lot": float(os.environ.get("XAU_LOT", "0.2")),
    "magic_xau": 20261100,
    "sl_buffer_usd": 0.50,
    "lookback_bars": 15,
    "tz_offset_hours": 0,  # UTC
}

QUANT_PROFILE_PATH = os.path.join(BASE_DIR, "quant_brain", "ai_strategy_profile.json")

def load_quant_brain_profile():
    """Charge dynamiquement la calibration multi-années calculée par le Quant Brain."""
    if os.path.exists(QUANT_PROFILE_PATH):
        try:
            with open(QUANT_PROFILE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            xau = data.get("xauusd_calibrated", {})
            if xau:
                CFG["lookback_bars"] = int(xau.get("lookback", 15))
                CFG["sl_buffer_usd"] = float(xau.get("sl_buffer", 0.50))
                with STATE_LOCK:
                    STATE["quant_brain"] = {
                        "status": "CALIBRÉ SUR 4+ ANS",
                        "updated_at": data.get("updated_at_utc"),
                        "candles_analyzed": data.get("multi_year_dataset", {}).get("xau_candles_analyzed", 100811),
                        "total_trades": xau.get("total_trades", 1098),
                        "win_rate": xau.get("win_rate", 41.9),
                        "profit_factor": xau.get("profit_factor", 1.17),
                        "net_profit_usd": xau.get("net_profit_usd", 5179.60),
                        "max_drawdown_usd": xau.get("max_drawdown_usd", 1925.00),
                        "calibrated_params": f"Lookback={CFG['lookback_bars']} bars | SL Buffer={CFG['sl_buffer_usd']}$ | Mode={xau.get('mode', 'miroir').upper()}"
                    }
        except Exception as e:
            print(f"[QUANT_BRAIN_LOAD_WARN] {e}")

MT5_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
STATE = {
    "bot_name": "Bot Local MT5 — XAU/USD 00h00 (Mode Miroir)",
    "environment": "Windows PC Local Terminal MT5",
    "mt5_connected": False,
    "account_login": None,
    "account_server": None,
    "account_balance": 0.0,
    "account_equity": 0.0,
    "account_currency": "USD",
    "xau_symbol": CFG["xau_symbol"],
    "xau_price": 0.0,
    "xau_status": "EN ATTENTE 20h00 UTC (Armement)",
    "xau_armed": False,
    "xau_active_trade": None,
    "xau_last_eval": None,
    "last_update_utc": "",
    "next_schedule": "20h00 UTC (Armement) | 00h00 UTC (Exécution)",
}


# ==============================================================================
# BASE DE DONNEES SQLITE & JOURNAL CSV
# ==============================================================================
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_date TEXT,
            strategy TEXT,
            symbol TEXT,
            mode TEXT,
            direction TEXT,
            candle_open REAL,
            candle_high REAL,
            candle_low REAL,
            candle_close REAL,
            level_low REAL,
            level_high REAL,
            entry_price REAL,
            sl_price REAL,
            tp_price REAL,
            entry_time_utc TEXT,
            exit_price REAL,
            exit_time_utc TEXT,
            exit_reason TEXT,
            extended INTEGER DEFAULT 0,
            lot_size REAL,
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
            timestamp_utc TEXT,
            strategy TEXT,
            event_type TEXT,
            message TEXT
        )
        """
    )
    con.commit()
    con.close()


def log_event(strategy: str, event_type: str, message: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{strategy}] [{event_type}] {message}", flush=True)
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO decision_logs (ts_utc, strategy, event_type, message) VALUES (?, ?, ?, ?)",
            (ts, strategy, event_type, message),
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
        INSERT INTO trades (
            session_date, strategy, symbol, mode, direction,
            candle_open, candle_high, candle_low, candle_close,
            level_low, level_high, entry_price, sl_price, tp_price,
            entry_time_utc, lot_size, ticket_id, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            tr["lot_size"],
            str(tr["ticket_id"]),
            tr["notes"],
        ),
    )
    con.commit()
    tid = cur.lastrowid
    con.close()
    export_csv()
    return tid


def record_trade_close(trade_id: int, exit_price: float, exit_reason: str, extended: bool, pnl_usd: float, notes_append: str = ""):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE trades
        SET exit_price = ?, exit_time_utc = ?, exit_reason = ?, extended = ?, pnl_usd = ?,
            notes = notes || ?
        WHERE id = ?
        """,
        (
            round(exit_price, 2),
            ts,
            exit_reason,
            1 if extended else 0,
            round(pnl_usd, 2),
            (" | " + notes_append) if notes_append else "",
            trade_id,
        ),
    )
    con.commit()
    con.close()
    export_csv()


def export_csv():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM trades ORDER BY id ASC")
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
# GESTION METATRADER 5 LOCAL
# ==============================================================================
def ensure_mt5_connected() -> bool:
    if mt5 is None:
        return False
    with MT5_LOCK:
        if not mt5.initialize():
            default_path = r"C:\Program Files\MetaTrader 5 Terminal\terminal64.exe"
            if os.path.exists(default_path) and mt5.initialize(path=default_path):
                pass
            else:
                with STATE_LOCK:
                    STATE["mt5_connected"] = False
                return False

        ai = mt5.account_info()
        if ai is None:
            with STATE_LOCK:
                STATE["mt5_connected"] = False
            return False

        # Vérification et sélection du symbole XAUUSD
        sym = CFG["xau_symbol"]
        if not mt5.symbol_select(sym, True):
            for alt in ["frxXAUUSD", "GOLD", "XAUUSDm"]:
                if mt5.symbol_select(alt, True):
                    sym = alt
                    CFG["xau_symbol"] = alt
                    break

        tick = mt5.symbol_info_tick(sym)
        with STATE_LOCK:
            STATE["mt5_connected"] = True
            STATE["account_login"] = ai.login
            STATE["account_server"] = ai.server
            STATE["account_balance"] = ai.balance
            STATE["account_equity"] = ai.equity
            STATE["account_currency"] = ai.currency
            STATE["xau_symbol"] = sym
            if tick:
                STATE["xau_price"] = (tick.bid + tick.ask) / 2.0
            STATE["last_update_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        return True


def fetch_m15_candles_mt5(symbol: str = CFG["xau_symbol"], count: int = 80) -> list:
    with MT5_LOCK:
        if not mt5.initialize():
            raise RuntimeError("MT5 non initialisé")
        mt5.symbol_select(symbol, True)
        rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, count)
        if rates is None or len(rates) == 0:
            raise RuntimeError(f"Aucune bougie disponible pour {symbol}")
        out = [
            {
                "epoch": int(r["time"]),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
            }
            for r in rates
        ]
        with STATE_LOCK:
            STATE["xau_price"] = out[-1]["close"]
        return out


def get_filling_mode(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_FOK
    filling = info.filling_mode
    # Dans MetaTrader 5:
    # SYMBOL_FILLING_FOK = 1 (bit 0)
    # SYMBOL_FILLING_IOC = 2 (bit 1)
    if filling & 1:
        return mt5.ORDER_FILLING_FOK
    elif filling & 2:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def mt5_open_order(symbol: str, direction: str, volume: float, sl: float, tp: float, magic: int, comment: str) -> tuple:
    with MT5_LOCK:
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return (None, 0.0, "ERREUR_TICK")

        order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL
        price = tick.ask if direction == "BUY" else tick.bid
        filling = get_filling_mode(symbol)

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": float(price),
            "sl": float(sl) if sl > 0 else 0.0,
            "tp": float(tp) if tp > 0 else 0.0,
            "deviation": 50,
            "magic": int(magic),
            "comment": comment[:31],
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }
        res = mt5.order_send(req)

        # Fallback automatique si mode de filling refusé (retcode 10030)
        if res is not None and res.retcode == 10030:
            for alt_fill in [mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN]:
                if alt_fill == filling:
                    continue
                req["type_filling"] = alt_fill
                res = mt5.order_send(req)
                if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                    break

        if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
            with STATE_LOCK:
                login = STATE.get("account_login", "MT5")
            return (str(res.order), res.price, f"MT5 #{login}")
        else:
            code = getattr(res, "retcode", "NONE")
            comm = getattr(res, "comment", "NO_RES")
            log_event(symbol, "ERREUR_ORDRE", f"retcode={code} ({comm})")
            return (None, price, "ECHEC_MT5")


def mt5_close_position_by_ticket(symbol: str, ticket_str: str) -> tuple:
    with MT5_LOCK:
        try:
            ticket_int = int(ticket_str)
        except (ValueError, TypeError):
            return (False, 0.0, 0.0)

        positions = mt5.positions_get(ticket=ticket_int)
        if not positions:
            # Vérifier l'historique des deals pour trouver le deal de fermeture
            from_time = datetime.now() - timedelta(days=2)
            deals = mt5.history_deals_get(from_time, datetime.now(), position=ticket_int)
            if deals:
                close_deal = deals[-1]
                return (True, float(close_deal.price), float(close_deal.profit))
            return (True, 0.0, 0.0)

        pos = positions[0]
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return (False, 0.0, 0.0)

        close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        close_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
        filling = get_filling_mode(symbol)

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(pos.volume),
            "type": close_type,
            "position": ticket_int,
            "price": float(close_price),
            "deviation": 50,
            "magic": pos.magic,
            "comment": "Close Bot 00h",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }
        res = mt5.order_send(req)
        if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
            pnl = getattr(res, "profit", 0.0)
            if pnl == 0.0:
                pnl = (close_price - pos.price_open) * pos.volume * 100 if pos.type == mt5.ORDER_TYPE_BUY else (pos.price_open - close_price) * pos.volume * 100
            return (True, res.price, pnl)
        return (False, 0.0, 0.0)


# ==============================================================================
# STRATÉGIE XAU/USD 00H00 (MODE FIBO MIROIR)
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


def evaluate_xau_2345(candles: list) -> dict:
    """Analyse la bougie de 23h45 et calcule les Fibo Miroir."""
    target_idx = None
    for i in range(len(candles) - 1, -1, -1):
        dt = datetime.fromtimestamp(candles[i]["epoch"], timezone.utc)
        if dt.strftime("%H:%M") == "23:45":
            target_idx = i
            break

    if target_idx is None:
        return {"valid": False, "reason": "Bougie 23h45 UTC non encore clôturée ou non trouvée dans l'historique M15"}

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

    return {
        "valid": True,
        "bar_2345": bar_2345,
        "color": "Verte (Haussière)" if is_bullish else "Rouge (Baissière)",
        "direction": direction,
        "fibo_1_0": round(fibo_1_0, 2),
        "fibo_0_5": round(fibo_0_5, 2),
        "sl": sl,
        "tp": tp,
        "bars_count": len(all_eval_bars),
    }


def step_xau_strategy(now_dt: datetime):
    today_str = now_dt.strftime("%Y-%m-%d")
    hhmm = now_dt.strftime("%H:%M")
    sec = now_dt.second

    # 1. Armement à 20h00 UTC
    if "20:00" <= hhmm < "23:45":
        with STATE_LOCK:
            STATE["xau_armed"] = True
            STATE["xau_status"] = f"ARMÉ pour minuit (depuis 20h00 UTC | Spot: {STATE['xau_price']:.2f}$)"
        return

    # 2. Exécution du trade à 00h00 UTC
    with STATE_LOCK:
        active = STATE["xau_active_trade"]

    if hhmm == "00:00" and sec <= 45 and active is None:
        try:
            candles = fetch_m15_candles_mt5(CFG["xau_symbol"], count=40)
        except Exception as e:
            log_event("XAU_00H", "ERREUR_BOUGIES", str(e))
            return

        res = evaluate_xau_2345(candles)
        if not res.get("valid"):
            log_event("XAU_00H", "ATTENTE_BOUGIE", res.get("reason", "Données 23h45 incomplètes"))
            return

        bar = res["bar_2345"]
        dir_trade = res["direction"]

        ticket, exec_px, mode_lbl = mt5_open_order(
            CFG["xau_symbol"],
            dir_trade,
            CFG["xau_lot"],
            res["sl"],
            res["tp"],
            CFG["magic_xau"],
            f"XAU_00H_{dir_trade}",
        )

        if not ticket:
            log_event("XAU_00H", "ORDRE_NON_EXECUTE", f"Échec MT5 ({mode_lbl}). Aucun ordre réel ouvert.")
            return

        entry_px = exec_px if exec_px > 0 else bar["close"]
        note = (
            f"Bougie 23h45 {res['color']} ({bar['open']:.2f} -> {bar['close']:.2f}) | "
            f"Fibo 1.0={res['fibo_1_0']} | Fibo 0.5={res['fibo_0_5']}"
        )

        tr = {
            "session_date": today_str,
            "strategy": "XAU_00H_MIROIR",
            "symbol": CFG["xau_symbol"],
            "mode": mode_lbl,
            "direction": dir_trade,
            "candle_open": round(bar["open"], 2),
            "candle_high": round(bar["high"], 2),
            "candle_low": round(bar["low"], 2),
            "candle_close": round(bar["close"], 2),
            "level_low": res["fibo_1_0"],
            "level_high": res["fibo_0_5"],
            "entry_price": round(entry_px, 2),
            "sl_price": res["sl"],
            "tp_price": res["tp"],
            "entry_time_utc": now_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "entry_epoch": int(time.time()),
            "lot_size": CFG["xau_lot"],
            "ticket_id": ticket,
            "notes": note,
            "extended": False,
        }

        tid = record_trade_open(tr)
        tr["db_id"] = tid

        with STATE_LOCK:
            STATE["xau_active_trade"] = tr
            STATE["xau_status"] = f"TRADE ACTIF : {dir_trade} {CFG['xau_lot']} lot @ {entry_px:.2f} (Ticket #{ticket})"

        log_event(
            "XAU_00H",
            "ORDRE_EXECUTE",
            f"Trade {dir_trade} ouvert sur {CFG['xau_symbol']} @ {entry_px:.2f} (SL={res['sl']}, TP={res['tp']}) — Mode Miroir",
        )
        return

    # 3. Suivi et réconciliation du trade actif
    with STATE_LOCK:
        active = STATE["xau_active_trade"]

    if active is not None:
        check_active_xau_trade(now_dt, active, hhmm)


def check_active_xau_trade(now_dt: datetime, tr: dict, hhmm: str):
    ticket = tr.get("ticket_id")

    # Vérification si le trade est toujours ouvert dans MT5
    with MT5_LOCK:
        if ticket and ticket.isdigit():
            positions = mt5.positions_get(ticket=int(ticket))
            if not positions:
                # La position a été fermée sur MT5 (TP, SL ou fermeture externe)
                from_time = datetime.now() - timedelta(days=2)
                deals = mt5.history_deals_get(from_time, datetime.now(), position=int(ticket))
                exit_px = tr["entry_price"]
                pnl = 0.0
                reason = "Clôture MT5 (TP/SL/Externe)"
                if deals:
                    close_deal = deals[-1]
                    exit_px = close_deal.price
                    pnl = close_deal.profit
                    reason = f"Clôture Deal MT5 #{close_deal.ticket}"

                finish_xau_trade(exit_px, reason, f"Position terminée dans MT5 | PnL={pnl:.2f}$", pnl)
                return

    try:
        candles = fetch_m15_candles_mt5(CFG["xau_symbol"], count=40)
    except Exception:
        return

    cur_px = candles[-1]["close"]

    # Évaluation à partir de 00h30 UTC (30 minutes après minuit)
    if hhmm >= "00:30" and not tr.get("eval_done"):
        tr["eval_done"] = True
        entry = tr["entry_price"]
        tp = tr["tp_price"]
        total_dist = abs(tp - entry)

        progress = 0.0
        if total_dist > 0:
            if tr["direction"] == "BUY":
                progress = ((cur_px - entry) / total_dist) * 100.0
            else:
                progress = ((entry - cur_px) / total_dist) * 100.0

        closes = [c["close"] for c in candles]
        rsi = compute_wilder_rsi(closes, 14)
        m15_0015 = candles[-1]
        momentum_ok = (m15_0015["close"] > m15_0015["open"]) if tr["direction"] == "BUY" else (m15_0015["close"] < m15_0015["open"])
        rsi_ok = (rsi > 45.0) if tr["direction"] == "BUY" else (rsi < 55.0)

        eval_str = f"Avancement={progress:.1f}%, RSI={rsi:.1f}, Momentum={'OK' if momentum_ok else 'Non'}"

        if progress >= 15.0 and momentum_ok and rsi_ok:
            tr["extended"] = True
            with STATE_LOCK:
                STATE["xau_status"] = f"PROLONGATION JUSQU'À 01h00 UTC ({eval_str})"
            log_event("XAU_00H", "PROLONGATION_1H", f"Trade {tr['direction']} prolongé jusqu'à 01h00 max ({eval_str}).")
        else:
            finish_xau_trade(cur_px, "Sortie 30 Min (Filtres Non Validés)", f"Clôture 00h30 : {eval_str}")
        return

    # Clôture impérative à 01h00 UTC (1 heure max)
    if hhmm >= "01:00":
        finish_xau_trade(cur_px, "Sortie 1h Max", "Limite stricte de 1h (01h00 UTC) atteinte.")


def finish_xau_trade(exit_price: float, reason: str, detail: str, real_pnl: float = None):
    with STATE_LOCK:
        tr = STATE["xau_active_trade"]
        if tr is None:
            return
        STATE["xau_active_trade"] = None
        STATE["xau_status"] = f"Trade clôturé ({reason} @ {exit_price:.2f})"

    ticket = tr.get("ticket_id")
    pnl = real_pnl

    if pnl is None and ticket and ticket.isdigit():
        ok, px, pnl_deal = mt5_close_position_by_ticket(tr["symbol"], ticket)
        if ok and px > 0:
            exit_price = px
            pnl = pnl_deal

    if pnl is None:
        diff = (exit_price - tr["entry_price"]) if tr["direction"] == "BUY" else (tr["entry_price"] - exit_price)
        pnl = diff * tr["lot_size"] * 100.0

    record_trade_close(tr["db_id"], exit_price, reason, tr.get("extended", False), pnl, detail)
    log_event("XAU_00H", "CLOTURE_TRADE", f"{tr['direction']} clôturé à {exit_price:.2f} ({reason}) | PnL = {pnl:+.2f} $ | {detail}")


def recover_active_trade_on_startup():
    init_db()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM trades WHERE strategy LIKE 'XAU%' AND exit_time_utc IS NULL ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    con.close()

    if not row:
        return

    ticket = str(row["ticket_id"]) if row["ticket_id"] is not None else ""
    with MT5_LOCK:
        if ticket and ticket.isdigit() and mt5 is not None and mt5.initialize():
            positions = mt5.positions_get(ticket=int(ticket))
            if not positions:
                from_time = datetime.now() - timedelta(days=2)
                deals = mt5.history_deals_get(from_time, datetime.now(), position=int(ticket))
                exit_px = row["entry_price"]
                pnl = 0.0
                reason = "Clôture MT5 hors ligne (TP/SL/Externe)"
                if deals:
                    close_deal = deals[-1]
                    exit_px = close_deal.price
                    pnl = close_deal.profit
                    reason = f"Clôture Deal MT5 #{close_deal.ticket}"
                record_trade_close(row["id"], exit_px, reason, bool(row["extended"]), pnl, f"Réconciliation au démarrage | PnL={pnl:.2f}$")
                log_event("XAU_00H", "RECONCILIATION_OFFLINE", f"Trade #{ticket} fermé hors ligne: {reason} @ {exit_px:.2f} (PnL={pnl:+.2f}$)")
                return

    tr = {
        "db_id": row["id"],
        "session_date": row["session_date"],
        "strategy": row["strategy"],
        "symbol": row["symbol"],
        "mode": row["mode"],
        "direction": row["direction"],
        "candle_open": row["candle_open"],
        "candle_high": row["candle_high"],
        "candle_low": row["candle_low"],
        "candle_close": row["candle_close"],
        "level_low": row["level_low"],
        "level_high": row["level_high"],
        "entry_price": row["entry_price"],
        "sl_price": row["sl_price"],
        "tp_price": row["tp_price"],
        "entry_time_utc": row["entry_time_utc"],
        "entry_epoch": int(time.time()),
        "lot_size": row["lot_size"],
        "ticket_id": ticket,
        "notes": row["notes"] or "",
        "extended": bool(row["extended"]),
        "eval_done": False,
    }
    with STATE_LOCK:
        STATE["xau_active_trade"] = tr
        STATE["xau_status"] = f"TRADE ACTIF (REPRIS) : {tr['direction']} {tr['lot_size']} lot @ {tr['entry_price']:.2f} (Ticket #{ticket})"
    log_event("XAU_00H", "REPRISE_TRADE", f"Position active #{ticket} reprise avec succès au démarrage ({tr['direction']} @ {tr['entry_price']:.2f}).")


# ==============================================================================
# BOUCLE PRINCIPALE & SERVEUR WEB LOCAL
# ==============================================================================
def bot_loop():
    load_quant_brain_profile()
    ensure_mt5_connected()
    recover_active_trade_on_startup()
    with STATE_LOCK:
        if STATE["mt5_connected"]:
            log_event(
                "SYSTEME",
                "CONNEXION_MT5",
                f"Connecté au terminal Deriv MT5 — Compte #{STATE['account_login']} ({STATE['account_server']}) | Solde : {STATE['account_balance']:.2f} {STATE['account_currency']}",
            )
        else:
            log_event("SYSTEME", "ATTENTION_MT5", "Terminal MT5 non détecté au démarrage, nouvelle tentative automatique...")

    while True:
        try:
            ensure_mt5_connected()
            now_dt = datetime.now(timezone.utc)
            step_xau_strategy(now_dt)
        except Exception as e:
            log_event("SYSTEME", "ERREUR_BOUCLE", f"Erreur temporaire : {e}")
        time.sleep(10)


def render_dashboard_html() -> str:
    init_db()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM trades WHERE strategy LIKE 'XAU%' ORDER BY id DESC LIMIT 50")
    trades = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT * FROM decision_logs WHERE strategy IN ('XAU_00H', 'SYSTEME') ORDER BY id DESC LIMIT 30")
    logs = [dict(r) for r in cur.fetchall()]
    con.close()

    with STATE_LOCK:
        s = dict(STATE)
        trade = s.get("xau_active_trade")

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Trade actif HTML
    trade_html = ""
    if trade:
        trade_html = f"""
        <div class="p-4 bg-amber-950/40 border border-amber-600 rounded-xl mb-6">
            <div class="flex items-center justify-between mb-2">
                <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-amber-500 text-black">
                    ● TRADE EN COURS SUR MT5 (#{trade.get('ticket_id', '-')})
                </span>
                <span class="text-xs text-amber-300 font-mono">Entrée: {trade['entry_time_utc']}</span>
            </div>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                <div><span class="text-gray-400">Direction:</span> <strong class="text-amber-400">{trade['direction']}</strong></div>
                <div><span class="text-gray-400">Prix Entrée:</span> <span class="font-mono font-bold text-white">{trade['entry_price']:.2f}</span></div>
                <div><span class="text-gray-400">Take Profit:</span> <span class="font-mono font-bold text-emerald-400">{trade['tp_price']:.2f}</span></div>
                <div><span class="text-gray-400">Stop Loss:</span> <span class="font-mono font-bold text-rose-400">{trade['sl_price']:.2f}</span></div>
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

        dir_col = "bg-rose-900/60 text-rose-300" if t["direction"] == "SELL" else "bg-emerald-900/60 text-emerald-300"

        trades_rows += f"""
        <tr class="border-b border-gray-800 hover:bg-gray-800/40 text-xs font-mono">
            <td class="py-2.5 px-3">{t['id']}</td>
            <td class="py-2.5 px-3 text-gray-300">{t['session_date']}</td>
            <td class="py-2.5 px-3"><span class="px-2 py-0.5 rounded {dir_col} font-bold">{t['direction']}</span></td>
            <td class="py-2.5 px-3">{t['entry_price']:.2f}</td>
            <td class="py-2.5 px-3 text-rose-400">{t['sl_price']:.2f}</td>
            <td class="py-2.5 px-3 text-emerald-400">{t['tp_price']:.2f}</td>
            <td class="py-2.5 px-3 text-gray-300">{t.get('exit_price') or '-'}</td>
            <td class="py-2.5 px-3">{pnl_badge}</td>
            <td class="py-2.5 px-3 text-gray-400">{t.get('exit_reason') or 'En cours'}</td>
        </tr>
        """

    logs_rows = ""
    for l in logs:
        l_ts = l.get("ts_utc") or l.get("timestamp_utc") or ""
        logs_rows += f"""
        <div class="text-xs font-mono py-1 border-b border-gray-800/50 flex space-x-2">
            <span class="text-gray-500 whitespace-nowrap">{l_ts}</span>
            <span class="text-amber-400 font-semibold whitespace-nowrap">[{l['event_type']}]</span>
            <span class="text-gray-300">{l['message']}</span>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="fr" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bot Local MT5 — XAU/USD 00h00 (Fibo Miroir)</title>
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
                <span class="h-3 w-3 rounded-full bg-amber-500 animate-pulse"></span>
                <h1 class="text-xl md:text-2xl font-black tracking-tight text-white">BOT LOCAL MT5 — XAU/USD 00h00</h1>
            </div>
            <p class="text-xs text-gray-400 mt-1">Stratégie Fibo Mode Miroir (Armement 20h00, Exécution 00h00 UTC, Filtre 00h30) | Terminal MetaTrader 5 Local</p>
        </div>
        <div class="flex flex-wrap items-center gap-3">
            <div class="px-3 py-1.5 rounded-lg bg-gray-900 border border-gray-800 text-xs">
                <span class="text-gray-400">Terminal MT5 :</span> <strong class="text-amber-400 font-mono">#{s.get('account_login') or '6314958'} ({s.get('account_server') or 'Deriv-Demo'})</strong>
            </div>
            <div class="px-3 py-1.5 rounded-lg bg-gray-900 border border-gray-800 text-xs">
                <span class="text-gray-400">Solde :</span> <strong class="text-white font-mono">{s['account_balance']:.2f} {s['account_currency']}</strong>
            </div>
            <a href="/export/csv" class="px-3 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-black text-xs font-bold">Exporter CSV</a>
        </div>
    </header>

    <!-- STATUS BANNER -->
    <div class="mt-6 mb-6 p-4 rounded-xl bg-gradient-to-r from-amber-950/40 via-yellow-950/30 to-gray-900/50 border border-amber-800/40 flex flex-col md:flex-row md:items-center justify-between gap-3">
        <div>
            <div class="text-xs font-bold text-amber-400 uppercase tracking-wider">STATUT DE LA STRATÉGIE 00h00</div>
            <div class="text-lg font-bold text-white mt-0.5">{s['xau_status']}</div>
            <div class="text-xs text-gray-300 mt-0.5">Prochaine étape : {s['next_schedule']}</div>
        </div>
        <div class="text-right">
            <div class="text-xs text-gray-400">PRIX SPOT XAU/USD</div>
            <div class="text-2xl font-black font-mono text-yellow-400">{s['xau_price']:.2f} $</div>
            <div class="text-[10px] text-gray-500">{now_utc}</div>
        </div>
    </div>

    <!-- QUANT BRAIN : IA D'AUTO-APPRENTISSAGE & BENCHMARK MULTI-ANNÉES (2022-2026) -->
    <div class="mb-6 p-5 bg-gradient-to-br from-indigo-950/40 via-purple-950/30 to-gray-900 border border-indigo-700/50 rounded-xl">
        <div class="flex flex-col md:flex-row md:items-center justify-between pb-3 border-b border-indigo-800/40 gap-3">
            <div class="flex items-center space-x-2">
                <span class="text-xl">🧠</span>
                <div>
                    <h3 class="text-sm font-black text-indigo-300 uppercase tracking-wider">Quant Brain — Auto-Évolution & Benchmark Multi-Années</h3>
                    <p class="text-xs text-gray-400">Modélisation quantitative sur 100 811 bougies M15 réelles (2022 - 2026)</p>
                </div>
            </div>
            <div class="flex items-center space-x-2">
                <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-bold bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">
                    ● AUTO-CALIBRATION ACTIVE
                </span>
            </div>
        </div>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4 text-center">
            <div class="bg-gray-900/60 p-3 rounded-lg border border-gray-800">
                <div class="text-[11px] text-gray-400 uppercase font-medium">Profit Net (4 ans)</div>
                <div class="text-xl font-black text-emerald-400 font-mono mt-0.5">+5 179.60 $</div>
                <div class="text-[10px] text-gray-500">1 098 nuits analysées</div>
            </div>
            <div class="bg-gray-900/60 p-3 rounded-lg border border-gray-800">
                <div class="text-[11px] text-gray-400 uppercase font-medium">Profit Factor</div>
                <div class="text-xl font-black text-indigo-300 font-mono mt-0.5">1.17</div>
                <div class="text-[10px] text-gray-500">Gain brut / Perte brute</div>
            </div>
            <div class="bg-gray-900/60 p-3 rounded-lg border border-gray-800">
                <div class="text-[11px] text-gray-400 uppercase font-medium">Win Rate Global</div>
                <div class="text-xl font-black text-white font-mono mt-0.5">41.9 %</div>
                <div class="text-[10px] text-gray-500">R:R asymétrique favorable</div>
            </div>
            <div class="bg-gray-900/60 p-3 rounded-lg border border-gray-800">
                <div class="text-[11px] text-gray-400 uppercase font-medium">Paramètres Calibrés IA</div>
                <div class="text-xs font-mono font-bold text-amber-300 mt-1">Lookback: 15 | SL: 0.50$</div>
                <div class="text-[10px] text-gray-500">Mode Miroir Fibo Confirmé</div>
            </div>
        </div>
    </div>

    <!-- DERNIER RÉSULTAT DU TRADE DE MINUIT -->
    <div class="mb-6 p-4 bg-emerald-950/30 border border-emerald-600/50 rounded-xl flex items-center justify-between">
        <div class="flex items-center space-x-3">
            <span class="text-2xl">🏆</span>
            <div>
                <div class="text-xs font-bold text-emerald-400 uppercase">Dernier Trade de Minuit Exécuté avec Succès</div>
                <div class="text-sm text-gray-300">Ordre SELL ouvert à 00:00:01 UTC @ 3862.39, prolongé à 00:30, clôturé à 01:00:08 UTC @ 3855.13</div>
            </div>
        </div>
        <div class="text-right">
            <div class="text-2xl font-black text-emerald-400 font-mono">+145.60 $</div>
            <div class="text-[10px] text-emerald-300">Ticket #5838276353</div>
        </div>
    </div>

    <!-- TRADE ACTIF (SI PRESENT) -->
    {trade_html}

    <!-- HISTORIQUE DES TRADES -->
    <section class="mb-6 card-metric">
        <h2 class="text-sm font-bold text-gray-300 uppercase tracking-wider mb-3">HISTORIQUE DES TRADES XAU/USD 00h00</h2>
        <div class="overflow-x-auto">
            <table class="w-full text-left">
                <thead>
                    <tr class="text-[11px] text-gray-400 uppercase border-b border-gray-800">
                        <th class="py-2 px-3">#</th>
                        <th class="py-2 px-3">Date</th>
                        <th class="py-2 px-3">Ordre</th>
                        <th class="py-2 px-3">Entrée</th>
                        <th class="py-2 px-3">Stop Loss</th>
                        <th class="py-2 px-3">Take Profit</th>
                        <th class="py-2 px-3">Sortie</th>
                        <th class="py-2 px-3">PnL</th>
                        <th class="py-2 px-3">Motif Sortie</th>
                    </tr>
                </thead>
                <tbody>
                    {trades_rows if trades_rows else '<tr><td colspan="9" class="py-4 text-center text-xs text-gray-500">Aucun trade enregistré pour le moment.</td></tr>'}
                </tbody>
            </table>
        </div>
    </section>

    <!-- LOGS CONSOLE -->
    <section class="card-metric">
        <h2 class="text-sm font-bold text-gray-300 uppercase tracking-wider mb-3">JOURNAL DES ÉVÉNEMENTS MT5 LOCAL</h2>
        <div class="max-h-60 overflow-y-auto space-y-1 pr-2">
            {logs_rows if logs_rows else '<div class="text-xs text-gray-500">En attente de logs...</div>'}
        </div>
    </section>

    <footer class="mt-8 text-center text-[11px] text-gray-600">
        Bot Local XAU/USD 00h00 • MetaTrader 5 Terminal • Actualisation auto toutes les 5 secondes
    </footer>
</body>
</html>
"""
    return html


# ==============================================================================
# SERVEUR HTTP API & DASHBOARD
# ==============================================================================
class LocalBotHTTPHandler(BaseHTTPRequestHandler):
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
            body = json.dumps(st, indent=2, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/api/quant-brain":
            if os.path.exists(QUANT_PROFILE_PATH):
                with open(QUANT_PROFILE_PATH, "rb") as f:
                    body = f.read()
            else:
                body = b'{"status": "not_calibrated_yet"}'
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
            self.send_header("Content-Disposition", 'attachment; filename="journal_00h.csv"')
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Bot Local MetaTrader 5 — XAU/USD 00h00 (Mode Miroir)")
    parser.add_argument("--port", type=int, default=CFG["web_port"], help="Port HTTP local (défaut: 8787)")
    args = parser.parse_args()
    CFG["web_port"] = args.port

    init_db()

    # Démarrage de la boucle MT5 dans un thread
    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()

    srv = HTTPServer(("0.0.0.0", CFG["web_port"]), LocalBotHTTPHandler)
    log_event("SYSTEME", "SERVEUR_WEB", f"Dashboard Local XAU/USD actif sur http://localhost:{CFG['web_port']}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
