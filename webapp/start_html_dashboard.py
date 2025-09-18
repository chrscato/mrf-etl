#!/usr/bin/env python3
"""
Start script for the HTML-based MRF dashboard
"""

import http.server
import socketserver
import webbrowser
import threading
import time
from pathlib import Path

def start_server(port=8080):
    """Start the HTTP server"""
    webapp_dir = Path(__file__).parent
    frontend_dir = webapp_dir / "frontend"
    
    # Change to frontend directory to serve files
    import os
    os.chdir(frontend_dir)
    
    class CustomHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
        def end_headers(self):
            # Add CORS headers for API calls
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            super().end_headers()
    
    with socketserver.TCPServer(("", port), CustomHTTPRequestHandler) as httpd:
        print(f"🌐 HTML Dashboard Server starting on port {port}")
        print(f"📁 Serving files from: {frontend_dir}")
        print(f"🔗 Dashboard URL: http://localhost:{port}")
        print("Press Ctrl+C to stop")
        
        # Open browser after a short delay
        def open_browser():
            time.sleep(1)
            webbrowser.open(f'http://localhost:{port}')
        
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down HTML dashboard server...")
            httpd.shutdown()

def main():
    """Main function to start the HTML dashboard"""
    print("🚀 Starting MRF HTML Dashboard...")
    print("=" * 50)
    
    # Try different ports if 8080 is busy
    ports_to_try = [8080, 8081, 8082, 8083, 8084]
    
    for port in ports_to_try:
        try:
            start_server(port)
            break
        except OSError as e:
            if "Address already in use" in str(e):
                print(f"Port {port} is busy, trying next port...")
                continue
            else:
                print(f"Error starting server: {e}")
                break
    else:
        print("❌ Could not find an available port. Please check if any servers are running.")
        print("Try: netstat -ano | findstr :8080")

if __name__ == "__main__":
    main()
