#!/usr/bin/env python3
"""
Complete dashboard launcher - starts both backend API and HTML frontend
"""

import subprocess
import sys
import time
import threading
import webbrowser
from pathlib import Path

def start_backend():
    """Start the FastAPI backend"""
    print("🔧 Starting FastAPI backend...")
    try:
        # Change to webapp directory and run start_backend.py
        webapp_dir = Path(__file__).parent
        subprocess.run([sys.executable, "start_backend.py"], cwd=webapp_dir, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Backend failed to start: {e}")
    except KeyboardInterrupt:
        print("\n🛑 Backend stopped")

def start_html_dashboard():
    """Start the HTML dashboard"""
    print("🌐 Starting HTML dashboard...")
    try:
        # Change to webapp directory and run start_html_dashboard.py
        webapp_dir = Path(__file__).parent
        subprocess.run([sys.executable, "start_html_dashboard.py"], cwd=webapp_dir, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ HTML dashboard failed to start: {e}")
    except KeyboardInterrupt:
        print("\n🛑 HTML dashboard stopped")

def main():
    """Main launcher function"""
    print("🚀 MRF Dashboard Launcher")
    print("=" * 50)
    print("This will start:")
    print("  🔧 FastAPI Backend (port 8000)")
    print("  🌐 HTML Dashboard (port 8080)")
    print("=" * 50)
    
    # Start backend in a separate thread
    backend_thread = threading.Thread(target=start_backend)
    backend_thread.daemon = True
    backend_thread.start()
    
    # Wait a moment for backend to start
    print("⏳ Waiting for backend to initialize...")
    time.sleep(3)
    
    # Start HTML dashboard in a separate thread
    dashboard_thread = threading.Thread(target=start_html_dashboard)
    dashboard_thread.daemon = True
    dashboard_thread.start()
    
    # Wait a moment for dashboard to start
    print("⏳ Waiting for dashboard to initialize...")
    time.sleep(2)
    
    print("\n✅ Dashboard is ready!")
    print("🔗 Backend API: http://localhost:8000")
    print("🌐 HTML Dashboard: http://localhost:8080")
    print("\nPress Ctrl+C to stop all services")
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down all services...")
        sys.exit(0)

if __name__ == "__main__":
    main()
