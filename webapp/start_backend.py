#!/usr/bin/env python3
"""
Start script for the FastAPI backend
"""

import uvicorn
import sys
from pathlib import Path

# Add the webapp directory to Python path
webapp_dir = Path(__file__).parent
sys.path.insert(0, str(webapp_dir))

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
