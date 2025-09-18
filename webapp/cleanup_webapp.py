#!/usr/bin/env python3
"""
Webapp Cleanup Script
Removes unnecessary files and consolidates the webapp structure
"""

import os
from pathlib import Path

def main():
    """Clean up unnecessary webapp files"""
    webapp_dir = Path(__file__).parent
    
    print("🧹 Cleaning up webapp files...")
    print("=" * 40)
    
    # Files to remove (redundant or unnecessary)
    files_to_remove = [
        "start_backend.py",      # Replaced by consolidated_dashboard.py
        "start_frontend.py",     # Replaced by consolidated_dashboard.py
        "start_webapp.py",       # Replaced by start_dashboard.py
        "start_html_dashboard.py", # Replaced by consolidated_dashboard.py
        "frontend/dashboard.py", # Streamlit version - replaced by HTML
        "frontend/index.html",   # Old HTML - replaced by optimized_dashboard.html
    ]
    
    removed_count = 0
    for file_path in files_to_remove:
        full_path = webapp_dir / file_path
        if full_path.exists():
            try:
                os.remove(full_path)
                print(f"✅ Removed: {file_path}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Error removing {file_path}: {e}")
        else:
            print(f"ℹ️  Not found: {file_path}")
    
    print("=" * 40)
    print(f"🎉 Cleanup complete! Removed {removed_count} files.")
    print("\n📁 Remaining files:")
    print("   • consolidated_dashboard.py - Main dashboard (backend + frontend)")
    print("   • start_dashboard.py - Simple launcher")
    print("   • frontend/optimized_dashboard.html - Optimized HTML dashboard")
    print("   • utils/optimized_queries.py - High-performance queries")
    print("   • backend/main.py - Original backend (kept for reference)")
    print("\n🚀 To start the dashboard, run:")
    print("   python start_dashboard.py")

if __name__ == "__main__":
    main()
