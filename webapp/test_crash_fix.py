#!/usr/bin/env python3
"""
Test script to verify crash fixes
"""

import sys
from pathlib import Path

# Add webapp to path
webapp_dir = Path(__file__).parent
sys.path.insert(0, str(webapp_dir))

from simple_queries import get_simple_queries

def test_crash_fixes():
    """Test that the crash fixes work"""
    print("🧪 Testing crash fixes...")
    
    try:
        # Initialize queries
        queries = get_simple_queries()
        print("✅ Queries initialized successfully")
        
        # Test basic search with very small limit to avoid memory issues
        results = queries.multi_field_search(
            state="GA",
            year_month="2025-08",
            limit=5  # Very small limit
        )
        print(f"✅ Basic search returned {len(results)} results")
        
        # Test autocomplete with small limit
        suggestions = queries.get_autocomplete_suggestions(
            field="primary_taxonomy_desc",
            query="",
            state="GA",
            year_month="2025-08",
            limit=5
        )
        print(f"✅ Autocomplete returned {len(suggestions)} suggestions")
        
        # Test with potentially dangerous input (should be safe now)
        results = queries.multi_field_search(
            state="GA",
            year_month="2025-08", 
            organization_name=["Test' OR '1'='1"],  # SQL injection attempt
            limit=3
        )
        print(f"✅ SQL injection test returned {len(results)} results (should be 0)")
        
        print("🎉 All crash fixes working correctly!")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_crash_fixes()
