#!/usr/bin/env python3
"""
Simple HTTP server to serve the KPI Architecture Visualization.
Run this script and open http://localhost:8080 in your browser.
"""

import http.server
import socketserver
import webbrowser
import os
import sys

PORT = int(os.environ.get('PORT', 6060))

def main():
    # Change to the visualization directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    Handler = http.server.SimpleHTTPRequestHandler
    
    # Set MIME types
    Handler.extensions_map.update({
        '.js': 'application/javascript',
        '.html': 'text/html',
        '.css': 'text/css',
    })
    
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        url = f"http://localhost:{PORT}"
        print("\n" + "="*60)
        print("  KPI Architecture Visualization Server")
        print("="*60)
        print(f"\n  Server running at: {url}")
        print(f"\n  Open this URL in your browser to view the visualization.")
        print("\n  Press Ctrl+C to stop the server.\n")
        print("="*60 + "\n")
        
        # Try to open browser automatically
        try:
            webbrowser.open(url)
        except:
            pass
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\nServer stopped.")
            sys.exit(0)

if __name__ == "__main__":
    main()
