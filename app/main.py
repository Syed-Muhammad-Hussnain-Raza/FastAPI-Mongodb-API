"""
@ Author: Syed Muhammad Hussnain Raza
Goodreads API - FastAPI + MongoDB (Motor)
"""

from fastapi import FastAPI, HTTPException, Query, Path, Depends, Header, Request
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import time
from collections import defaultdict
from datetime import datetime, timedelta

# -----------------------------
# Config
# -----------------------------
MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "goodreadsDB"
API_KEY = "secret123"

# -----------------------------
# DB client
# -----------------------------
client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

# -----------------------------
# In-Memory Storage for Rate Limiting and Metrics
# -----------------------------
rate_limit_store: Dict[str, List[float]] = defaultdict(list)
metrics_store = {
    "total_requests": 0,
    "requests_by_endpoint": defaultdict(int),
    "latency_histogram": defaultdict(int),  # buckets: <50ms, <100ms, <500ms, <1000ms, >1000ms
    "status_codes": defaultdict(int)
}

# -----------------------------
# Models
# -----------------------------
class Book(BaseModel):
    book_id: int
    title: str
    authors: Optional[str] = None
    average_rating: Optional[float] = None
    ratings_count: Optional[int] = None
    original_publication_year: Optional[int] = None

class Rating(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(..., ge=1, le=5)

class Tag(BaseModel):
    tag_id: int
    tag_name: str

# -----------------------------
# App
# -----------------------------
app = FastAPI(title="Goodreads API", version="1.0.0")

# -----------------------------
# Helper Functions
# -----------------------------
def update_metrics(endpoint: str, latency_ms: float, status_code: int):
    """Update metrics store with request data"""
    metrics_store["total_requests"] += 1
    metrics_store["requests_by_endpoint"][endpoint] += 1
    metrics_store["status_codes"][status_code] += 1
    
    # Latency histogram buckets
    if latency_ms < 50:
        metrics_store["latency_histogram"]["<50ms"] += 1
    elif latency_ms < 100:
        metrics_store["latency_histogram"]["<100ms"] += 1
    elif latency_ms < 500:
        metrics_store["latency_histogram"]["<500ms"] += 1
    elif latency_ms < 1000:
        metrics_store["latency_histogram"]["<1000ms"] += 1
    else:
        metrics_store["latency_histogram"][">1000ms"] += 1

def check_rate_limit(ip: str, limit: int = 60, window_seconds: int = 60) -> bool:
    """Check if IP has exceeded rate limit. Returns True if allowed."""
    now = time.time()
    cutoff = now - window_seconds
    
    # Clean old entries
    rate_limit_store[ip] = [ts for ts in rate_limit_store[ip] if ts > cutoff]
    
    # Check limit
    if len(rate_limit_store[ip]) >= limit:
        return False
    
    # Add current request
    rate_limit_store[ip].append(now)
    return True

async def paginate_with_query(collection_name: str, query: dict, sort_field: Optional[str], sort_dir: int, page: int, page_size: int):
    """Paginate query results"""
    coll = db[collection_name]
    total = await coll.count_documents(query)
    cursor = coll.find(query, {"_id": 0})
    if sort_field:
        cursor = cursor.sort(sort_field, sort_dir)
    docs = await cursor.skip((page-1)*page_size).limit(page_size).to_list(length=page_size)
    return {"items": docs, "page": page, "page_size": page_size, "total": total}

# -----------------------------
# Middleware: Logging, Rate Limiting, and Metrics
# -----------------------------
@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start = time.time()
    client_ip = request.client.host if request.client else "unknown"
    
    # Rate limiting check
    if not check_rate_limit(client_ip):
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Maximum 60 requests per minute."}
        )
    
    # Process request
    response = await call_next(request)
    latency_ms = round((time.time() - start) * 1000, 2)
    
    # Logging
    entry = {
        "route": request.url.path,
        "params": dict(request.query_params),
        "status": response.status_code,
        "latency_ms": latency_ms,
        "client_ip": client_ip,
        "ts": time.time()
    }
    print(entry)
    
    # Update metrics
    update_metrics(request.url.path, latency_ms, response.status_code)
    
    return response

# -----------------------------
# Auth dependency
# -----------------------------
async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return x_api_key

# -----------------------------
# Health and Metrics Endpoints
# -----------------------------
@app.get("/healthz")
async def health_check():
    """Health check endpoint with MongoDB ping and build info"""
    try:
        # Ping MongoDB
        await db.command("ping")
        mongo_status = "healthy"
        
        # Get MongoDB server info
        server_info = await db.command("buildInfo")
        mongo_version = server_info.get("version", "unknown")
        
    except Exception as e:
        mongo_status = f"unhealthy: {str(e)}"
        mongo_version = "unknown"
    
    return {
        "status": "healthy" if mongo_status == "healthy" else "degraded",
        "timestamp": time.time(),
        "mongodb": {
            "status": mongo_status,
            "version": mongo_version
        },
        "api_version": "1.0.0"
    }

@app.get("/metrics")
async def get_metrics():
    """Get API metrics including request counters and latency histograms"""
    return {
        "total_requests": metrics_store["total_requests"],
        "requests_by_endpoint": dict(metrics_store["requests_by_endpoint"]),
        "status_codes": dict(metrics_store["status_codes"]),
        "latency_histogram": dict(metrics_store["latency_histogram"]),
        "timestamp": time.time()
    }

# -----------------------------
# Root + Debug endpoints
# -----------------------------
@app.get("/")
async def root():
    return {"message": "Goodreads API running", "ts": time.time()}

@app.get("/debug/counts")
async def debug_counts():
    counts = {}
    for name in ["books", "ratings", "tags", "book_tags", "to_read"]:
        try:
            counts[name] = await db[name].count_documents({})
        except Exception:
            counts[name] = None
    return {"db": DB_NAME, "counts": counts}

# -----------------------------
# Core Book Endpoints
# -----------------------------
@app.get("/books")
async def get_books(
    q: Optional[str] = Query(None, description="search in title or authors"),
    tag: Optional[str] = Query(None, description="filter by tag name (partial, case-insensitive)"),
    min_avg: Optional[float] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    sort: Optional[str] = Query("title", description="one of: avg, ratings_count, year, title"),
    order: Optional[str] = Query("asc", description="asc or desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
):
    query: Dict[str, Any] = {}
    if q:
        query["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]

    if min_avg is not None:
        query.setdefault("average_rating", {})
        query["average_rating"]["$gte"] = min_avg

    if year_from is not None or year_to is not None:
        yq: Dict[str, int] = {}
        if year_from is not None:
            yq["$gte"] = year_from
        if year_to is not None:
            yq["$lte"] = year_to
        query["original_publication_year"] = yq

    if tag:
        tag_docs = await db.tags.find({"tag_name": {"$regex": tag, "$options": "i"}}, {"tag_id": 1}).to_list(length=100)
        tag_ids = [t["tag_id"] for t in tag_docs]
        if tag_ids:
            bts = await db.book_tags.find({"tag_id": {"$in": tag_ids}}, {"goodreads_book_id": 1}).to_list(length=None)
            goodreads_ids = [bt["goodreads_book_id"] for bt in bts]
            if goodreads_ids:
                query["goodreads_book_id"] = {"$in": goodreads_ids}
            else:
                return {"items": [], "page": page, "page_size": page_size, "total": 0}
        else:
            return {"items": [], "page": page, "page_size": page_size, "total": 0}

    sort_map = {"avg": "average_rating", "ratings_count": "ratings_count", "year": "original_publication_year", "title": "title"}
    sort_field = sort_map.get(sort, "title")
    sort_dir = 1 if order == "asc" else -1

    return await paginate_with_query("books", query, sort_field, sort_dir, page, page_size)

@app.get("/books/{book_id}")
async def get_book(book_id: int = Path(...)):
    book = await db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return book

@app.get("/books/{book_id}/tags")
async def get_book_tags(book_id: int):
    book = await db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    goodreads_book_id = book.get("goodreads_book_id") or book.get("book_id")

    pipeline = [
        {"$match": {"goodreads_book_id": goodreads_book_id}},
        {"$lookup": {
            "from": "tags",
            "localField": "tag_id",
            "foreignField": "tag_id",
            "as": "tag_info"
        }},
        {"$unwind": "$tag_info"},
        {"$project": {"_id": 0, "tag_id": 1, "tag_name": "$tag_info.tag_name", "count": 1}}
    ]

    tags = await db.book_tags.aggregate(pipeline).to_list(length=100)
    return {"tags": tags}

@app.get("/books/{book_id}/ratings/summary")
async def get_ratings_summary(book_id: int):
    book = await db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {"_id": "$rating", "count": {"$sum": 1}}}
    ]
    results = await db.ratings.aggregate(pipeline).to_list(length=None)
    histogram = {r["_id"]: r["count"] for r in results}
    histogram = {i: histogram.get(i, 0) for i in range(1, 6)}

    avg_pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {"_id": None, "avg": {"$avg": "$rating"}, "count": {"$sum": 1}}}
    ]
    avg_result = await db.ratings.aggregate(avg_pipeline).to_list(length=1)
    if not avg_result:
        return {"book_id": book_id, "avg": 0, "count": 0, "histogram": histogram}

    return {"book_id": book_id, "avg": round(avg_result[0]["avg"], 2), "count": int(avg_result[0]["count"]), "histogram": histogram}

# -----------------------------
# Author Endpoints
# -----------------------------
@app.get("/authors/{author_name}/books")
async def get_books_by_author(author_name: str):
    query = {"authors": {"$regex": author_name, "$options": "i"}}
    cursor = db.books.find(query, {"_id": 0}).limit(200)
    results = await cursor.to_list(length=200)
    return results

# -----------------------------
# Tag Endpoints
# -----------------------------
@app.get("/tags")
async def get_tags(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100)):
    pipeline = [
        {"$lookup": {"from": "book_tags", "localField": "tag_id", "foreignField": "tag_id", "as": "bts"}},
        {"$project": {"_id": 0, "tag_id": 1, "tag_name": 1, "book_count": {"$size": "$bts"}}},
        {"$skip": (page-1)*page_size},
        {"$limit": page_size}
    ]
    tags = await db.tags.aggregate(pipeline).to_list(length=page_size)
    total = await db.tags.count_documents({})
    return {"items": tags, "page": page, "page_size": page_size, "total": total}

# -----------------------------
# User Endpoints
# -----------------------------
@app.get("/users/{user_id}/to-read")
async def get_to_read(user_id: int):
    docs = await db.to_read.find({"user_id": user_id}, {"_id": 0}).to_list(length=500)
    book_ids = [d["book_id"] for d in docs]
    if not book_ids:
        return []
    books = await db.books.find({"book_id": {"$in": book_ids}}, {"_id": 0, "book_id": 1, "title": 1, "authors": 1}).to_list(length=len(book_ids))
    return books

@app.get("/users/{user_id}/recommendations")
async def get_recommendations(
    user_id: int,
    top_k: int = Query(20, ge=1, le=100, description="Number of recommendations to return")
):
    """
    Get book recommendations based on user's favorite tags.
    Algorithm: Find user's top-rated books, extract their tags, recommend highly-rated books with similar tags.
    """
    # Get user's ratings (4-5 stars only)
    user_ratings = await db.ratings.find(
        {"user_id": user_id, "rating": {"$gte": 4}},
        {"book_id": 1, "_id": 0}
    ).to_list(length=500)
    
    if not user_ratings:
        # Fallback: return top-rated books overall
        books = await db.books.find(
            {"average_rating": {"$gte": 4.0}},
            {"_id": 0}
        ).sort("average_rating", -1).limit(top_k).to_list(length=top_k)
        return {"recommendations": books, "algorithm": "top_rated_overall"}
    
    user_book_ids = [r["book_id"] for r in user_ratings]
    
    # Get goodreads_book_ids for these books
    user_books = await db.books.find(
        {"book_id": {"$in": user_book_ids}},
        {"goodreads_book_id": 1, "book_id": 1, "_id": 0}
    ).to_list(length=len(user_book_ids))
    
    goodreads_ids = [b.get("goodreads_book_id", b.get("book_id")) for b in user_books]
    
    # Find tags associated with these books
    user_tags = await db.book_tags.find(
        {"goodreads_book_id": {"$in": goodreads_ids}},
        {"tag_id": 1, "_id": 0}
    ).to_list(length=None)
    
    if not user_tags:
        # Fallback: return top-rated books
        books = await db.books.find(
            {"average_rating": {"$gte": 4.0}},
            {"_id": 0}
        ).sort("average_rating", -1).limit(top_k).to_list(length=top_k)
        return {"recommendations": books, "algorithm": "top_rated_overall"}
    
    # Count tag frequencies
    tag_counts = defaultdict(int)
    for ut in user_tags:
        tag_counts[ut["tag_id"]] += 1
    
    # Get top tags
    top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    top_tag_ids = [t[0] for t in top_tags]
    
    # Find books with these tags
    recommended_book_tags = await db.book_tags.find(
        {"tag_id": {"$in": top_tag_ids}},
        {"goodreads_book_id": 1, "_id": 0}
    ).to_list(length=None)
    
    recommended_goodreads_ids = list(set([bt["goodreads_book_id"] for bt in recommended_book_tags]))
    
    # Get book details, exclude already read books
    pipeline = [
        {"$match": {
            "goodreads_book_id": {"$in": recommended_goodreads_ids},
            "book_id": {"$nin": user_book_ids},
            "average_rating": {"$gte": 3.5}
        }},
        {"$sort": {"average_rating": -1, "ratings_count": -1}},
        {"$limit": top_k},
        {"$project": {"_id": 0}}
    ]
    
    recommendations = await db.books.aggregate(pipeline).to_list(length=top_k)
    
    return {
        "recommendations": recommendations,
        "algorithm": "tag_based",
        "user_favorite_tags": top_tag_ids[:5]
    }

# -----------------------------
# Rating Endpoints
# -----------------------------
@app.post("/ratings")
async def add_rating(rating: Rating, _key: str = Depends(verify_api_key)):
    book = await db.books.find_one({"book_id": rating.book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    res = await db.ratings.update_one(
        {"user_id": rating.user_id, "book_id": rating.book_id},
        {"$set": rating.dict()},
        upsert=True
    )
    status_code = 201 if res.upserted_id else 200
    return JSONResponse(status_code=status_code, content={
        "status": "success",
        "message": "Rating created" if res.upserted_id else "Rating updated",
        "matched": res.matched_count,
        "modified": res.modified_count,
        "upserted": bool(res.upserted_id)
    })