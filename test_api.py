"""
Simple test script to verify the enhanced in-memory database functionality
"""

import requests
import json

# Test API endpoints
BASE_URL = "http://localhost:8000"

def test_health():
    """Test API health"""
    response = requests.get(f"{BASE_URL}/api/health")
    print("🏥 Health Check:")
    print(json.dumps(response.json(), indent=2))
    return response.status_code == 200

def test_jobs_list():
    """Test jobs listing"""
    response = requests.get(f"{BASE_URL}/api/jobs")
    print("\n📋 Jobs List:")
    print(json.dumps(response.json(), indent=2))
    return response.status_code == 200

def test_file_upload():
    """Test file upload with sample data"""
    # Create a simple CSV data
    csv_content = """name,age,email,city
John Doe,25,john@example.com,New York
Jane Smith,30,jane@example.com,Los Angeles
Bob Johnson,35,bob@example.com,Chicago
Alice Brown,28,alice@example.com,Houston
Charlie Wilson,32,charlie@example.com,Phoenix"""
    
    # Create form data
    files = {'file': ('test_data.csv', csv_content, 'text/csv')}
    data = {'action': 'validate'}
    
    response = requests.post(f"{BASE_URL}/api/upload", files=files, data=data)
    print("\n📤 File Upload:")
    print(json.dumps(response.json(), indent=2))
    
    if response.status_code == 200:
        job_id = response.json().get('job_id')
        return job_id
    return None

def test_job_status(job_id):
    """Test job status monitoring"""
    if not job_id:
        print("\n❌ No job ID to check status")
        return
        
    # Wait a moment for processing to start
    import time
    time.sleep(2)
        
    response = requests.get(f"{BASE_URL}/api/status/{job_id}")
    print(f"\n📊 Job Status ({job_id}):")
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"❌ Status code: {response.status_code}")
        print(f"Response: {response.text}")

def main():
    print("🧪 Testing Enhanced In-Memory Database API")
    print("=" * 50)
    
    # Test health
    health_ok = test_health()
    if not health_ok:
        print("❌ Health check failed!")
        return
    
    # Test initial jobs list (should be empty)
    test_jobs_list()
    
    # Test file upload
    job_id = test_file_upload()
    
    # Test job status
    test_job_status(job_id)
    
    # Test jobs list again (should have one job)
    import time
    time.sleep(1)  # Give the job a moment to process
    test_jobs_list()
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    main()
