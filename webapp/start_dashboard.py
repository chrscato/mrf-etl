#!/usr/bin/env python3
"""
MRF Dashboard Launcher - Single Terminal Solution
Starts the consolidated dashboard with all optimizations
"""

import sys
import subprocess
from pathlib import Path

def main():
    """Main launcher function"""
    print("🚀 MRF Dashboard Launcher")
    print("=" * 50)
    print("📊 Starting Consolidated Dashboard...")
    print("🔧 Backend: FastAPI with optimized queries")
    print("🎨 Frontend: Optimized HTML/JS")
    print("⚡ Performance: Materialized views + indexing")
    print("🌐 Single Process: No multiple terminals needed!")
    print("=" * 50)
    
    # Get the webapp directory
    webapp_dir = Path(__file__).parent
    
    # Start the consolidated dashboard
    try:
        subprocess.run([
            sys.executable, 
            str(webapp_dir / "consolidated_dashboard.py")
        ], cwd=webapp_dir)
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error starting dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()