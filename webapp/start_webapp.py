#!/usr/bin/env python3
"""
Start both backend and frontend services for the MRF webapp
"""

import subprocess
import sys
import time
import signal
import os
from pathlib import Path

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\nShutting down webapp...")
    sys.exit(0)

def main():
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    webapp_dir = Path(__file__).parent
    
    print("🚀 Starting MRF Data Lookup Webapp")
    print("=" * 50)
    
    # Start backend
    print("📡 Starting FastAPI backend...")
    backend_process = subprocess.Popen([
        sys.executable, "start_backend.py"
    ], cwd=webapp_dir)
    
    # Wait a moment for backend to start
    time.sleep(3)
    
    # Start frontend
    print("🎨 Starting Streamlit frontend...")
    frontend_process = subprocess.Popen([
        sys.executable, "start_frontend.py"
    ], cwd=webapp_dir)
    
    print("\n✅ Webapp started successfully!")
    print("=" * 50)
    print("🌐 Dashboard: http://localhost:8501")
    print("🔌 API Docs: http://localhost:8000/docs")
    print("📊 API Health: http://localhost:8000/api/health")
    print("=" * 50)
    print("Press Ctrl+C to stop all services")
    
    try:
        # Wait for both processes
        backend_process.wait()
        frontend_process.wait()
    except KeyboardInterrupt:
        print("\n🛑 Stopping services...")
        backend_process.terminate()
        frontend_process.terminate()
        
        # Wait for graceful shutdown
        try:
            backend_process.wait(timeout=5)
            frontend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("⚠️  Force killing processes...")
            backend_process.kill()
            frontend_process.kill()
        
        print("✅ All services stopped")

if __name__ == "__main__":
    main()
