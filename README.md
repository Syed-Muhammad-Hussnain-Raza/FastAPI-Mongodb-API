# Goodreads API - Assignment 01

MongoDB-backed API for GoodBooks-10k dataset.

## Author
Syed Muhammad Hussnain Raza

## Setup
1. Install: `pip install fastapi motor uvicorn`
2. Start MongoDB: `mongod`
3. Ingest data: `python ingest/ingest_data.py`
4. Go to app folder: `cd app` 
5. Run API: `uvicorn main:app --reload`
6. Docs: http://localhost:8000/docs

## Features Implemented

### Required API Endpoints:
- All 8 endpoints with full filtering, sorting, pagination
- API key authentication on POST /ratings
- JSONL logging with all required fields
- Pydantic validation
- Proper error handling

### Optional EndPoints Added:
1. Health check (`/healthz`) - MongoDB ping + version
2. Metrics (`/metrics`) - Request counters + latency histogram
3. Rate limiting - 60 requests/minute per IP
4. Recommendations - Tag-based collaborative filtering

## Design Decisions
- Rate limiting excludes health/metrics endpoints (monitoring best practice)
- `/users/{user_id}/to-read` enhanced to join with books for better UX
- Metrics stored in-memory (reset on restart) - production would use Redis
- Recommendations use tag-based CF with multiple fallbacks

## API Key
Default: `secret123` (change in production via env vars)