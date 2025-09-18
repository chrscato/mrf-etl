#!/usr/bin/env python3
"""
Start script for the Streamlit frontend
"""

import subprocess
import sys
from pathlib import Path

def main():
    webapp_dir = Path(__file__).parent
    frontend_script = webapp_dir / "frontend" / "dashboard.py"
    
    # Try different ports if 8501 is busy
    ports_to_try = [8501, 8502, 8503, 8504, 8505]
    
    for port in ports_to_try:
        cmd = [
            sys.executable, "-m", "streamlit", "run",
            str(frontend_script),
            "--server.port", str(port),
            "--server.address", "0.0.0.0",
            "--browser.gatherUsageStats", "false"
        ]
        
        print(f"Starting Streamlit dashboard on port {port}...")
        print(f"Dashboard will be available at: http://localhost:{port}")
        print("Press Ctrl+C to stop")
        
        try:
            subprocess.run(cmd, check=True)
            break  # If successful, exit the loop
        except subprocess.CalledProcessError as e:
            if port == ports_to_try[-1]:  # Last port
                print(f"Error starting dashboard on all ports: {e}")
                print("Please check if any Streamlit processes are running and kill them:")
                print("  - Windows: taskkill /f /im python.exe")
                print("  - Or try: netstat -ano | findstr :8501")
                sys.exit(1)
            else:
                print(f"Port {port} is busy, trying next port...")
                continue
        except KeyboardInterrupt:
            print("\nShutting down dashboard...")
            break

if __name__ == "__main__":
    main()
