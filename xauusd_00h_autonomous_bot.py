#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Contrôleur et Sélecteur d'Environnement Multi-Services :
  - Sur Render.com (Service 1 : bot00h)      : Lance le Bot EUR/USD Cloud (Session US 13h00 - 21h45 UTC)
  - Sur Render.com (Service 2 : bot00h-gold) : Lance le Bot XAU/USD Cloud (Minuit 00h00 Fibo Miroir)
  - Sur PC Local (Windows MT5)               : Lance le Bot XAU/USD Local MT5 (#6314958)
"""

import os
import sys
import platform
import argparse

def is_cloud_environment() -> bool:
    if os.environ.get("RENDER") == "true" or os.environ.get("RENDER") == "1":
        return True
    if os.environ.get("PORT") == "10000" or os.environ.get("PORT") == "10001":
        return True
    if platform.system().lower() != "windows":
        return True
    return False

def main():
    parser = argparse.ArgumentParser(description="Bot Trader — Sélecteur d'Environnement Cloud / Local")
    parser.add_argument(
        "--mode",
        choices=["auto", "eurusd_cloud", "xauusd_cloud", "xauusd_local"],
        default=os.environ.get("BOT_MODE", "auto"),
        help="Mode d'exécution : auto, eurusd_cloud, xauusd_cloud, ou xauusd_local",
    )
    parser.add_argument("--port", type=int, default=None, help="Port d'écoute du dashboard HTTP")
    args, unknown = parser.parse_known_args()

    mode = args.mode
    if mode == "auto":
        mode = "eurusd_cloud" if is_cloud_environment() else "xauusd_local"

    print("=" * 70, flush=True)
    print(f"[SYSTEME] [DISPATCHEUR] Mode sélectionné : {mode.upper()}", flush=True)
    print(f"[SYSTEME] [SYSTEME_OS]   {platform.system()} ({platform.platform()})", flush=True)
    print("=" * 70, flush=True)

    if mode == "xauusd_cloud":
        print("[SYSTEME] Lancement du BOT CLOUD DERIV — XAU/USD GOLD (Minuit 00h00 Fibo Miroir)...", flush=True)
        import xauusd_cloud_bot
        if args.port:
            xauusd_cloud_bot.CFG["web_port"] = args.port
        xauusd_cloud_bot.main()
    elif mode == "eurusd_cloud":
        print("[SYSTEME] Lancement du BOT CLOUD DERIV — EUR/USD (Session US 13h00 - 21h45 UTC)...", flush=True)
        import eurusd_cloud_bot
        if args.port:
            eurusd_cloud_bot.CFG["web_port"] = args.port
        eurusd_cloud_bot.main()
    else:
        print("[SYSTEME] Lancement du BOT LOCAL MT5 — XAU/USD 00h00 (Fibo Mode Miroir)...", flush=True)
        import xauusd_local_bot
        if args.port:
            xauusd_local_bot.CFG["web_port"] = args.port
        xauusd_local_bot.main()

if __name__ == "__main__":
    main()
