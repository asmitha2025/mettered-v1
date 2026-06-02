#!/usr/bin/env python3
"""
run_dashboard.py - Interactive Dashboard Launcher
-------------------------------------------------
Starts the FastAPI/Uvicorn web server and opens the dashboard in the
system browser.

Usage:
  python run_dashboard.py
"""

import os
import socket
import sys
import threading
import time
import webbrowser

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

try:
    import uvicorn
except ImportError:
    print("  [ERROR] Missing dashboard dependency: uvicorn")
    print("  Install project dependencies first:")
    print("  python -m pip install -r requirements.txt")
    sys.exit(1)


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def find_free_port(start_port: int = 8000, max_attempts: int = 100) -> int:
    for port in range(start_port, start_port + max_attempts):
        if not is_port_in_use(port):
            return port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def open_browser(url: str, delay_seconds: float = 1.2) -> None:
    time.sleep(delay_seconds)
    print(f"\n  [INFO] Opening dashboard: {url}\n", flush=True)
    webbrowser.open(url)


def main() -> None:
    print("""
+--------------------------------------------------------+
|   SubIntel Dashboard Launcher                          |
|   Interactive SaaS Billing Analytics and ETL Jobs       |
+--------------------------------------------------------+
    """, flush=True)

    port = find_free_port(8000)
    url = f"http://127.0.0.1:{port}"

    browser_thread = threading.Thread(target=open_browser, args=(url,), daemon=True)
    browser_thread.start()

    print(f"  [INFO] Starting FastAPI server on 127.0.0.1:{port}...", flush=True)
    try:
        uvicorn.run("src.web.app:app", host="127.0.0.1", port=port, log_level="info")
    except KeyboardInterrupt:
        print("\n  [INFO] Dashboard server stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
