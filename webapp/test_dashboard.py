#!/usr/bin/env python3
"""
Test script for the consolidated dashboard
Verifies that all components work correctly
"""

import requests
import time
import sys
from pathlib import Path

def test_dashboard():
    """Test the consolidated dashboard functionality"""
    print("🧪 Testing MRF Consolidated Dashboard...")
    print("=" * 50)
    
    # Test configuration
    base_url = "http://localhost:8080"
    test_timeout = 30  # seconds
    
    # Test cases
    tests = [
        {
            "name": "Health Check",
            "url": f"{base_url}/api/health",
            "expected_status": 200,
            "check_data": True
        },
        {
            "name": "Main Dashboard",
            "url": f"{base_url}/",
            "expected_status": 200,
            "check_data": False
        },
        {
            "name": "Category Statistics",
            "url": f"{base_url}/api/explore/category-stats?state=GA&year_month=2025-08",
            "expected_status": 200,
            "check_data": True
        },
        {
            "name": "Data Availability",
            "url": f"{base_url}/api/explore/data-availability?state=GA&year_month=2025-08&category=payer&limit=5",
            "expected_status": 200,
            "check_data": True
        },
        {
            "name": "Multi-field Search",
            "url": f"{base_url}/api/search/multi-field?state=GA&year_month=2025-08&limit=10",
            "expected_status": 200,
            "check_data": True
        }
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        print(f"Testing {test['name']}...", end=" ")
        
        try:
            response = requests.get(test['url'], timeout=test_timeout)
            
            if response.status_code == test['expected_status']:
                if test['check_data']:
                    data = response.json()
                    if data and len(data) > 0:
                        print("✅ PASS")
                        passed += 1
                    else:
                        print("❌ FAIL - No data returned")
                        failed += 1
                else:
                    print("✅ PASS")
                    passed += 1
            else:
                print(f"❌ FAIL - Status {response.status_code}")
                failed += 1
                
        except requests.exceptions.ConnectionError:
            print("❌ FAIL - Connection refused (dashboard not running)")
            failed += 1
        except requests.exceptions.Timeout:
            print("❌ FAIL - Request timeout")
            failed += 1
        except Exception as e:
            print(f"❌ FAIL - {str(e)}")
            failed += 1
    
    print("=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Dashboard is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Check the dashboard logs.")
        return False

def main():
    """Main test function"""
    print("🚀 MRF Dashboard Test Suite")
    print("Make sure the dashboard is running: python start_dashboard.py")
    print("Waiting 5 seconds for dashboard to start...")
    time.sleep(5)
    
    success = test_dashboard()
    
    if success:
        print("\n✅ Dashboard is ready to use!")
        print("🌐 Open your browser to: http://localhost:8080")
    else:
        print("\n❌ Dashboard has issues. Check the logs and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()
