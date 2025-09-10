"""
FastAPI backend for Data Ingestion Pipeline
Provides REST API endpoints for file upload, validation, and pipeline execution
"""

import os
import sys
import uuid
import asyncio
import tempfile
import threading
import json
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

from services.file_processor import FileProcessor
from services.data_validator import DataValidator
from services.data_reporter import DataQualityReporter
from services.kafka_producer_enhanced import EnhancedKafkaProducer
from data_ingestion_pipeline import DataIngestionPipeline

# Initialize FastAPI app
app = FastAPI(
    title="Data Ingestion Pipeline API",
    description="API for intelligent data validation and ingestion with Kafka integration",
    version="1.0.0"
)

# Add CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://localhost:8080"],  # Vite and CRA default ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
file_processor = FileProcessor()
data_validator = DataValidator()
reporter = DataQualityReporter()
pipeline = DataIngestionPipeline()

# Enhanced In-memory Database for job persistence
import threading

class InMemoryJobDB:
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()
    
    def create_job(self, job_id: str, filename: str, action: str) -> Dict[str, Any]:
        """Create a new job entry"""
        with self.lock:
            job_data = {
                'job_id': job_id,
                'filename': filename,
                'action': action,
                'status': 'pending',
                'progress': 0,
                'message': 'Job created, waiting to start...',
                'result': None,
                'error': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            self.jobs[job_id] = job_data
            return job_data.copy()
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job by ID"""
        with self.lock:
            job = self.jobs.get(job_id)
            if job:
                # Convert numpy types before returning
                return convert_numpy_types(job.copy())
            return None
    
    def update_job(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """Update a job with new data"""
        with self.lock:
            if job_id in self.jobs:
                # Convert numpy types to native Python types
                clean_updates = convert_numpy_types(updates)
                self.jobs[job_id].update(clean_updates)
                self.jobs[job_id]['updated_at'] = datetime.now().isoformat()
                return True
            return False
    
    def list_jobs(self) -> List[Dict[str, Any]]:
        """Get all jobs, newest first"""
        with self.lock:
            jobs = list(self.jobs.values())
            # Sort by created_at descending
            jobs.sort(key=lambda x: x['created_at'], reverse=True)
            # Convert numpy types before returning
            return [convert_numpy_types(job.copy()) for job in jobs]
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        with self.lock:
            if job_id in self.jobs:
                del self.jobs[job_id]
                return True
            return False
    
    def get_job_count(self) -> int:
        """Get total number of jobs"""
        with self.lock:
            return len(self.jobs)

# Global in-memory database instance
job_db = InMemoryJobDB()

# Legacy compatibility
job_status: Dict[str, Dict[str, Any]] = {}

# JSON serialization helper for NumPy types
def convert_numpy_types(obj):
    """Convert NumPy types to Python native types for JSON serialization"""
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj

# Pydantic models for request/response
class ValidationRequest(BaseModel):
    quality_threshold: float = 0.8
    output_format: str = "json"

class KafkaRequest(BaseModel):
    topic: str
    quality_threshold: float = 0.8

class PipelineRequest(BaseModel):
    topic: str = "data_ingestion_topic"
    quality_threshold: float = 0.8
    timeout_seconds: int = 30

class JobStatus(BaseModel):
    job_id: str
    status: str  # "pending", "running", "completed", "failed"
    progress: int  # 0-100
    message: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Data Ingestion Pipeline API",
        "status": "running",
        "version": "1.0.0",
        "endpoints": {
            "upload": "/api/upload",
            "validate": "/api/validate/{job_id}",
            "kafka": "/api/kafka/{job_id}",
            "pipeline": "/api/pipeline/{job_id}",
            "status": "/api/status/{job_id}",
            "download": "/api/download/{job_id}/{file_type}"
        }
    }

@app.get("/api/health")
async def health_check():
    """Detailed health check"""
    return {
        "api": "healthy",
        "services": {
            "file_processor": "ready",
            "data_validator": "ready", 
            "kafka_producer": "ready",
            "reporter": "ready"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    action: str = "validate"  # "validate", "kafka", "pipeline"
):
    """
    Upload a file and start processing
    Returns job_id for tracking progress
    """
    # Validate file
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    # Create job entry in database
    job_data = job_db.create_job(job_id, file.filename, action)
    
    # Save uploaded file temporarily
    temp_dir = Path("temp_uploads")
    temp_dir.mkdir(exist_ok=True)
    
    file_path = temp_dir / f"{job_id}_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Update job with file path
        job_db.update_job(job_id, {
            "file_path": str(file_path),
            "message": "File saved, processing queued"
        })
        
        # Start background processing based on action
        if action == "validate":
            background_tasks.add_task(process_validation, job_id, str(file_path))
        elif action == "kafka":
            background_tasks.add_task(process_kafka, job_id, str(file_path))
        elif action == "pipeline":
            background_tasks.add_task(process_full_pipeline, job_id, str(file_path))
        else:
            raise HTTPException(status_code=400, detail=f"Invalid action: {action}")
        
        return {"job_id": job_id, "status": "accepted", "message": "Processing started"}
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

async def process_validation(job_id: str, file_path: str):
    """Background task for file validation"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting validation..."
        })
        
        # Load and validate file
        df = file_processor.load_data_from_file(file_path)
        job_db.update_job(job_id, {
            "progress": 30,
            "message": f"Loaded {len(df)} records"
        })
        
        records = file_processor.convert_dataframe_to_records(df, clean_data=True)
        job_db.update_job(job_id, {
            "progress": 50,
            "message": "Running intelligent validation..."
        })
        
        # Validate data
        validation_results = data_validator.validate_batch(records, file_path)
        job_db.update_job(job_id, {
            "progress": 80,
            "message": "Generating report..."
        })
        
        # Generate statistics
        stats = data_validator.get_statistics()
        good_data = data_validator.get_good_data()
        bad_data = data_validator.get_bad_data()
        
        # Create output directory
        output_dir = Path(f"api_output/{job_id}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate report
        report_path = reporter.generate_comprehensive_report(
            validation_results,
            stats,
            output_format="html"
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Validation completed successfully",
            "result": {
                "total_records": len(records),
                "valid_records": len(good_data),
                "invalid_records": len(bad_data),
                "success_rate": f"{(len(good_data) / len(records) * 100):.2f}%" if records else "0%",
                "statistics": stats,
                "report_path": str(report_path),
                "schema_info": data_validator.get_schema_info() if hasattr(data_validator, 'get_schema_info') else None
            }
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

async def process_kafka(job_id: str, file_path: str, topic: str = "data_ingestion_api"):
    """Background task for Kafka sending"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting Kafka processing..."
        })
        
        # Run file-to-kafka pipeline
        results = pipeline.run_file_to_kafka_pipeline(
            input_file=file_path,
            topic=topic,
            validate_before_send=True
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Data sent to Kafka successfully",
            "result": results
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

async def process_full_pipeline(job_id: str, file_path: str, topic: str = "data_ingestion_api"):
    """Background task for full pipeline"""
    try:
        job_db.update_job(job_id, {
            "status": "running",
            "progress": 10,
            "message": "Starting full pipeline..."
        })
        
        # Create output directory
        output_dir = f"api_output/{job_id}"
        
        # Run full pipeline
        results = pipeline.run_full_pipeline(
            input_file=file_path,
            topic=topic,
            quality_threshold=0.8,
            timeout_seconds=30,
            output_dir=output_dir
        )
        
        job_db.update_job(job_id, {
            "progress": 100,
            "status": "completed",
            "message": "Full pipeline completed successfully",
            "result": results
        })
        
    except Exception as e:
        job_db.update_job(job_id, {
            "status": "failed",
            "error": str(e)
        })

@app.get("/api/status/{job_id}")
async def get_job_status(job_id: str):
    """Get job status and progress"""
    job = job_db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job

@app.get("/api/jobs")
async def list_jobs():
    """List all jobs"""
    jobs = job_db.list_jobs()
    return {
        "jobs": jobs,
        "count": len(jobs)
    }

@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its files"""
    job = job_db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Clean up files
    try:
        if "file_path" in job:
            Path(job["file_path"]).unlink(missing_ok=True)
        
        # Remove output directory
        output_dir = Path(f"api_output/{job_id}")
        if output_dir.exists():
            import shutil
            shutil.rmtree(output_dir)
        
        # Remove job from database
        job_db.delete_job(job_id)
        
        return {"message": "Job deleted successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

@app.get("/api/download/{job_id}/{file_type}")
async def download_file(job_id: str, file_type: str):
    """Download generated files (report, data, etc.)"""
    job = job_db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    
    output_dir = Path(f"api_output/{job_id}")
    
    if file_type == "report":
        # Find HTML report
        html_files = list(output_dir.glob("*.html"))
        if html_files:
            return FileResponse(html_files[0], filename=f"report_{job_id}.html")
    elif file_type == "good_data":
        # Find good data CSV
        csv_files = list(output_dir.glob("good_data.csv"))
        if csv_files:
            return FileResponse(csv_files[0], filename=f"good_data_{job_id}.csv")
    elif file_type == "all_data":
        # Find processed data
        csv_files = list(output_dir.glob("high_quality_data.csv"))
        if csv_files:
            return FileResponse(csv_files[0], filename=f"processed_data_{job_id}.csv")
    
    raise HTTPException(status_code=404, detail=f"File type '{file_type}' not found")

@app.get("/api/quarantine")
async def get_quarantined_items():
    """Get all quarantined items from completed jobs"""
    jobs = job_db.list_jobs()
    quarantined_items = []
    
    for job in jobs:
        if job["status"] == "completed" and job.get("result", {}).get("invalid_records", 0) > 0:
            quarantined_items.append({
                "id": job["job_id"],
                "filename": job["filename"],
                "reason": f"Validation failed: {job['result']['invalid_records']} invalid records",
                "severity": "high" if job["result"]["invalid_records"] > job["result"]["total_records"] * 0.1 else "medium",
                "records": job["result"]["invalid_records"],
                "total_records": job["result"]["total_records"],
                "quarantine_date": job["updated_at"],
                "risk_score": max(0, 100 - int(float(job["result"]["success_rate"].replace("%", ""))))
            })
    
    return {"quarantined_items": quarantined_items, "count": len(quarantined_items)}

@app.get("/api/statistics")
async def get_statistics():
    """Get overall system statistics"""
    jobs = job_db.list_jobs()
    
    total_jobs = len(jobs)
    completed_jobs = len([j for j in jobs if j["status"] == "completed"])
    failed_jobs = len([j for j in jobs if j["status"] == "failed"])
    running_jobs = len([j for j in jobs if j["status"] == "running"])
    
    total_records = sum([j.get("result", {}).get("total_records", 0) for j in jobs if j["status"] == "completed"])
    valid_records = sum([j.get("result", {}).get("valid_records", 0) for j in jobs if j["status"] == "completed"])
    invalid_records = sum([j.get("result", {}).get("invalid_records", 0) for j in jobs if j["status"] == "completed"])
    
    return {
        "jobs": {
            "total": total_jobs,
            "completed": completed_jobs,
            "failed": failed_jobs,
            "running": running_jobs
        },
        "records": {
            "total": total_records,
            "valid": valid_records,
            "invalid": invalid_records,
            "success_rate": f"{(valid_records / total_records * 100):.2f}%" if total_records > 0 else "0%"
        }
    }

# Serve the React frontend (for production)
@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    # Create necessary directories
    Path("temp_uploads").mkdir(exist_ok=True)
    Path("api_output").mkdir(exist_ok=True)
    
    print("🚀 Data Ingestion Pipeline API started!")
    print("📊 Frontend available at: http://localhost:8000")
    print("📝 API docs available at: http://localhost:8000/docs")

# Mount static files for production (commented out for development)
# frontend_dist = Path("frontend/dist")
# if frontend_dist.exists():
#     app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")

if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
