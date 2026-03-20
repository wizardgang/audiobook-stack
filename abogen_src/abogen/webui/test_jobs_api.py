import sys
import os
import json
from pathlib import Path

# Add abogen to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from abogen.webui.app import create_app

def run_test():
    app = create_app()
    app.testing = True
    client = app.test_client()
    
    # 1. Test POST /api/jobs with file or text
    print("Testing POST /api/jobs")
    
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    if file_path and os.path.exists(file_path):
        print(f"Uploading file: {file_path}")
        with open(file_path, "rb") as f:
            data = {"file": f, "title": "Test File Upload", "voice": "af_heart"}
            res = client.post("/api/jobs", data=data, content_type="multipart/form-data")
    else:
        print("No file provided, using default text payload...")
        payload = {
            "text": "This is a small test chunk of the book.",
            "title": "Test Chunk API",
            "voice": "af_heart",
        }
        res = client.post("/api/jobs", json=payload)
        
    print(f"Status: {res.status_code}")
    print(f"Data: {res.data.decode()}")
    
    if res.status_code != 201:
        print("Failed to create job!")
        return
        
    data = json.loads(res.data)
    job_id = data["job_id"]
    print(f"Created Job ID: {job_id}")
    
    # 2. Test GET /api/jobs
    print("\nTesting GET /api/jobs")
    res = client.get("/api/jobs")
    print(f"Status: {res.status_code}")
    jobs_data = json.loads(res.data)
    assert any(j["id"] == job_id for j in jobs_data["jobs"])
    print(f"Found {len(jobs_data['jobs'])} jobs")
    
    # 3. Test GET /api/jobs/<job_id>
    print(f"\nTesting GET /api/jobs/{job_id}")
    res = client.get(f"/api/jobs/{job_id}")
    print(f"Status: {res.status_code}")
    job_info = json.loads(res.data)
    print(f"Job Info: {json.dumps(job_info, indent=2)}")
    
    # 4. Test DELETE /api/jobs/<job_id>
    print(f"\nTesting DELETE /api/jobs/{job_id}")
    res = client.delete(f"/api/jobs/{job_id}")
    print(f"Status: {res.status_code}")
    print(f"Data: {res.data.decode()}")
    
    # Verify it's gone
    res = client.get(f"/api/jobs/{job_id}")
    print(f"Status after delete: {res.status_code}")
    
    print("\nAPI test successful!")

if __name__ == "__main__":
    run_test()
