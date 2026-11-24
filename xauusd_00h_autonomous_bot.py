#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Contrôleur et Sélecteur d'Environnement Multi-Services :
  - Sur Render.com (Mode Dual Cloud par défaut) : Lance SIMULTANÉMENT les 2 Bots Cloud Deriv
      * Bot 1 : EUR/USD (Session US 13h00 - 21h45 UTC) -> http://.../
      * Bot 2 : XAU/USD Gold (Minuit 00h00 Fibo Miroir) -> http://.../gold
  - Sur PC Local (Windows MT5)                  : Lance le Bot XAU/USD Local MT5 (#6314958) -> http://localhost:8787
"""

import os
import sys
import json
import time
import platform
import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

def is_cloud_environment() -> bool:
    if os.environ.get("RENDER") == "true" or os.environ.get("RENDER") == "1":
        return True
    if os.environ.get("PORT") == "10000" or os.environ.get("PORT") == "10001":
        return True
    if platform.system().lower() != "windows":
        return True
    return False

import eurusd_cloud_bot
import xauusd_cloud_bot

class UnifiedCloudHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/gold", "/gold/", "/xau", "/xauusd", "/gold/dashboard", "/xau/dashboard"):
            body = xauusd_cloud_bot.render_dashboard_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path in ("/gold/export/csv", "/xauusd_cloud_journal.csv"):
            if os.path.exists(xauusd_cloud_bot.CSV_PATH):
                with open(xauusd_cloud_bot.CSV_PATH, "rb") as f:
                    content = f.read()
            else:
                content = b"id,session_date,symbol,direction,entry_price,tp_price,sl_price,exit_price,pnl_usd,exit_reason\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="xauusd_cloud_journal.csv"')
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        if self.path in ("/export/csv", "/eurusd_journal.csv"):
            if os.path.exists(eurusd_cloud_bot.CSV_PATH):
                with open(eurusd_cloud_bot.CSV_PATH, "rb") as f:
                    content = f.read()
            else:
                content = b"id,session_date,contract_id,direction,entry_price,support_level,tp1_level,tp2_level,exit_price,pnl_usd,exit_reason\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="eurusd_journal.csv"')
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        if self.path == "/api/status":
            with eurusd_cloud_bot.STATE_LOCK:
                st_eur = dict(eurusd_cloud_bot.STATE)
            with xauusd_cloud_bot.STATE_LOCK:
                st_xau = dict(xauusd_cloud_bot.STATE)
            payload = json.dumps({"eurusd": st_eur, "xauusd": st_xau}, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        # Par défaut : Dashboard EUR/USD
        body = eurusd_cloud_bot.render_dashboard_html().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return

def run_dual_cloud(port: int):
    print("=" * 70, flush=True)
    print(f"[SYSTEME] LANCEMENT DU MODE DUAL-CLOUD (LES 2 BOTS ACTIFS SUR RENDER)", flush=True)
    print(f"[SYSTEME]   • Bot 1 : EUR/USD Session US (13h-21h45 UTC) -> URL /", flush=True)
    print(f"[SYSTEME]   • Bot 2 : XAU/USD Gold Minuit (Fibo Miroir)    -> URL /gold", flush=True)
    print(f"[SYSTEME]   • Port HTTP Écoute : {port}", flush=True)
    print("=" * 70, flush=True)

    eurusd_cloud_bot.init_db()
    xauusd_cloud_bot.init_db()

    # Démarrage des 2 boucles de trading en arrière-plan
    t_eur = threading.Thread(target=eurusd_cloud_bot.bot_loop, daemon=True, name="EURUSD_Cloud_Thread")
    t_xau = threading.Thread(target=xauusd_cloud_bot.bot_loop, daemon=True, name="XAUUSD_Cloud_Thread")
    t_eur.start()
    t_xau.start()

    srv = HTTPServer(("0.0.0.0", port), UnifiedCloudHTTPHandler)
    print(f"[SERVEUR_WEB] Serveur Unifié Cloud actif sur http://0.0.0.0:{port}", flush=True)
    srv.serve_forever()

def main():
    parser = argparse.ArgumentParser(description="Bot Trader — Sélecteur d'Environnement Cloud / Local")
    parser.add_argument(
        "--mode",
        choices=["auto", "dual_cloud", "eurusd_cloud", "xauusd_cloud", "xauusd_local"],
        default=os.environ.get("BOT_MODE", "auto"),
        help="Mode d'exécution : auto, dual_cloud, eurusd_cloud, xauusd_cloud, ou xauusd_local",
    )
    parser.add_argument("--port", type=int, default=None, help="Port d'écoute du dashboard HTTP")
    args, unknown = parser.parse_known_args()

    mode = args.mode
    if mode == "auto":
        mode = "dual_cloud" if is_cloud_environment() else "xauusd_local"

    print("=" * 70, flush=True)
    print(f"[SYSTEME] [DISPATCHEUR] Mode sélectionné : {mode.upper()}", flush=True)
    print(f"[SYSTEME] [SYSTEME_OS]   {platform.system()} ({platform.platform()})", flush=True)
    print("=" * 70, flush=True)

    web_port = args.port or int(os.environ.get("PORT", "10000"))

    if mode == "dual_cloud":
        run_dual_cloud(web_port)
    elif mode == "xauusd_cloud":
        print("[SYSTEME] Lancement du BOT CLOUD DERIV — XAU/USD GOLD...", flush=True)
        xauusd_cloud_bot.CFG["web_port"] = web_port
        xauusd_cloud_bot.main()
    elif mode == "eurusd_cloud":
        print("[SYSTEME] Lancement du BOT CLOUD DERIV — EUR/USD...", flush=True)
        eurusd_cloud_bot.CFG["web_port"] = web_port
        eurusd_cloud_bot.main()
    else:
        print("[SYSTEME] Lancement du BOT LOCAL MT5 — XAU/USD 00h00 (Fibo Mode Miroir)...", flush=True)
        import xauusd_local_bot
        if args.port:
            xauusd_local_bot.CFG["web_port"] = args.port
        xauusd_local_bot.main()

if __name__ == "__main__":
    main()
