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

MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "goodreadsDB"
API_KEY = "secret123"
RATE_LIMIT_REQUESTS = 60
RATE_LIMIT_WINDOW = 60  # seconds

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

rate_limit_store: Dict[str, List[float]] = defaultdict(list)
metrics_store = {
    "total_requests": 0,
    "requests_by_endpoint": defaultdict(int),
    "latency_histogram": defaultdict(int),
    "status_codes": defaultdict(int)
}

class Book(BaseModel):
    book_id: int
    title: str
    authors: Optional[str] = None
    average_rating: Optional[float] = None
    ratings_count: Optional[int] = None
    original_publication_year: Optional[int] = None
    goodreads_book_id: Optional[int] = None

class Rating(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5")

class Tag(BaseModel):
    tag_id: int
    tag_name: str

class PaginatedResponse(BaseModel):
    items: List[dict]
    page: int
    page_size: int
    total: int


app = FastAPI(
    title="Goodreads API",
    version="1.0.0",
    description="MongoDB-backed REST API for GoodBooks-10k dataset"
)

# helper functions
def update_metrics(endpoint: str, latency_ms: float, status_code: int):
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

def check_rate_limit(ip: str, limit: int = RATE_LIMIT_REQUESTS, window_seconds: int = RATE_LIMIT_WINDOW) -> bool:

    now = time.time()
    cutoff = now - window_seconds
    
    # Clean old entries outside the time window
    rate_limit_store[ip] = [ts for ts in rate_limit_store[ip] if ts > cutoff]
    
    # Check if limit exceeded
    if len(rate_limit_store[ip]) >= limit:
        return False
    
    # Add current request timestamp
    rate_limit_store[ip].append(now)
    return True

async def paginate_with_query(
    collection_name: str,
    query: dict,
    sort_field: Optional[str],
    sort_dir: int,
    page: int,
    page_size: int
) -> Dict[str, Any]:
    """Helper function to paginate query results"""
    coll = db[collection_name]
    total = await coll.count_documents(query)
    cursor = coll.find(query, {"_id": 0})
    if sort_field:
        cursor = cursor.sort(sort_field, sort_dir)
    docs = await cursor.skip((page-1)*page_size).limit(page_size).to_list(length=page_size)
    return {"items": docs, "page": page, "page_size": page_size, "total": total}

# Middleware: Logging, Rate Limiting, and Metrics
@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start = time.time()
    client_ip = request.client.host if request.client else "unknown"
    
    # Rate limiting - skip for monitoring endpoints
    excluded_paths = ["/healthz", "/metrics", "/debug/counts", "/docs", "/openapi.json"]
    if request.url.path not in excluded_paths:
        if not check_rate_limit(client_ip):
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded. Maximum {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds."
                }
            )
    
    # Process request
    response = await call_next(request)
    latency_ms = round((time.time() - start) * 1000, 2)
    
    # JSONL Logging
    log_entry = {
        "route": request.url.path,
        "params": dict(request.query_params),
        "status": response.status_code,
        "latency_ms": latency_ms,
        "client_ip": client_ip,
        "ts": time.time()
    }
    print(log_entry)  # In production, write to file or logging service
    
    # Update metrics
    update_metrics(request.url.path, latency_ms, response.status_code)
    
    return response

# Authentication
async def verify_api_key(x_api_key: str = Header(..., description="API key for authentication")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return x_api_key

# Health Check
@app.get(
    "/healthz",
    tags=["Monitoring"],
    summary="Health check endpoint",
    description="Returns service health status with MongoDB connectivity check"
)
async def health_check():
    try:
        # Ping MongoDB
        await db.command("ping")
        mongo_status = "healthy"
        
        # Get MongoDB server info
        server_info = await db.command("buildInfo")
        mongo_version = server_info.get("version", "unknown")
        
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "mongodb": {
                "status": mongo_status,
                "version": mongo_version
            },
            "api_version": "1.0.0"
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": time.time(),
                "mongodb": {
                    "status": f"error: {str(e)}",
                    "version": "unknown"
                },
                "api_version": "1.0.0"
            }
        )

# Metrics
@app.get(
    "/metrics",
    tags=["Monitoring"],
    summary="API metrics",
    description="Returns request counters and latency histograms"
)

async def get_metrics():
    return {
        "total_requests": metrics_store["total_requests"],
        "requests_by_endpoint": dict(metrics_store["requests_by_endpoint"]),
        "status_codes": dict(metrics_store["status_codes"]),
        "latency_histogram": dict(metrics_store["latency_histogram"]),
        "timestamp": time.time(),
        "note": "Metrics are stored in-memory and reset on server restart"
    }

# Root and Debug Endpoints
@app.get("/", tags=["General"])
async def root():
    return {
        "message": "Goodreads API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/healthz",
        "metrics": "/metrics",
        "timestamp": time.time()
    }

@app.get("/debug/counts", tags=["Debug"], summary="Collection counts")
async def debug_counts():
    counts = {}
    for name in ["books", "ratings", "tags", "book_tags", "to_read"]:
        try:
            counts[name] = await db[name].count_documents({})
        except Exception:
            counts[name] = None
    return {"database": DB_NAME, "collections": counts}

# GET /books
@app.get(
    "/books",
    response_model=PaginatedResponse,
    tags=["Books"],
    summary="List books with filters",
    description="Search and filter books with pagination, sorting, and tag filtering"
)
async def get_books(
    q: Optional[str] = Query(None, description="Search in title or authors"),
    tag: Optional[str] = Query(None, description="Filter by tag name"),
    min_avg: Optional[float] = Query(None, description="Minimum average rating"),
    year_from: Optional[int] = Query(None, description="Publication year from (inclusive)"),
    year_to: Optional[int] = Query(None, description="Publication year to (inclusive)"),
    sort: Optional[str] = Query("title", description="Sort field: avg, ratings_count, year, title"),
    order: Optional[str] = Query("asc", description="Sort order: asc or desc"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page")
):
    
    query: Dict[str, Any] = {}
    
    # Text search in title or authors
    if q:
        query["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]
    
    # Minimum average rating filter
    if min_avg is not None:
        query["average_rating"] = {"$gte": min_avg}
    
    # Year range filter
    if year_from is not None or year_to is not None:
        year_query: Dict[str, int] = {}
        if year_from is not None:
            year_query["$gte"] = year_from
        if year_to is not None:
            year_query["$lte"] = year_to
        query["original_publication_year"] = year_query
    
    # Tag filter (requires joining through book_tags)
    if tag:
        # Find matching tags
        tag_docs = await db.tags.find(
            {"tag_name": {"$regex": tag, "$options": "i"}},
            {"tag_id": 1}
        ).to_list(length=100)
        
        tag_ids = [t["tag_id"] for t in tag_docs]
        
        if tag_ids:
            # Find books with these tags
            book_tag_docs = await db.book_tags.find(
                {"tag_id": {"$in": tag_ids}},
                {"goodreads_book_id": 1}
            ).to_list(length=None)
            
            goodreads_ids = list(set([bt["goodreads_book_id"] for bt in book_tag_docs]))
            
            if goodreads_ids:
                query["goodreads_book_id"] = {"$in": goodreads_ids}
            else:
                # No books found with this tag
                return {"items": [], "page": page, "page_size": page_size, "total": 0}
        else:
            # Tag not found
            return {"items": [], "page": page, "page_size": page_size, "total": 0}
    
    # Sorting
    sort_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count",
        "year": "original_publication_year",
        "title": "title"
    }
    sort_field = sort_map.get(sort, "title")
    sort_dir = 1 if order == "asc" else -1
    
    return await paginate_with_query("books", query, sort_field, sort_dir, page, page_size)

# GET /books/{book_id}
@app.get(
    "/books/{book_id}",
    response_model=Book,
    tags=["Books"],
    summary="Get book by ID"
)
async def get_book(book_id: int = Path(..., description="Book ID")):
    """Get detailed information about a specific book"""
    book = await db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return book

# GET /books/{book_id}/tags
@app.get(
    "/books/{book_id}/tags",
    tags=["Books"],
    summary="Get book tags"
)
async def get_book_tags(book_id: int = Path(..., description="Book ID")):
    book = await db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    # Get goodreads_book_id (fallback to book_id if not present)
    goodreads_book_id = book.get("goodreads_book_id", book_id)
    
    # Aggregation pipeline to join book_tags with tags
    pipeline = [
        {"$match": {"goodreads_book_id": goodreads_book_id}},
        {"$lookup": {
            "from": "tags",
            "localField": "tag_id",
            "foreignField": "tag_id",
            "as": "tag_info"
        }},
        {"$unwind": "$tag_info"},
        {"$project": {
            "_id": 0,
            "tag_id": "$tag_info.tag_id",
            "tag_name": "$tag_info.tag_name",
            "count": 1
        }},
        {"$sort": {"count": -1}}
    ]
    
    tags = await db.book_tags.aggregate(pipeline).to_list(length=100)
    return {"book_id": book_id, "tags": tags}

# GET /authors/{author_name}/books
@app.get(
    "/authors/{author_name}/books",
    tags=["Authors"],
    summary="Get books by author"
)
async def get_books_by_author(author_name: str = Path(..., description="Author name")):
    query = {"authors": {"$regex": author_name, "$options": "i"}}
    cursor = db.books.find(query, {"_id": 0}).limit(200)
    results = await cursor.to_list(length=200)
    
    if not results:
        return []
    
    return results

# GET /tags
@app.get(
    "/tags",
    response_model=PaginatedResponse,
    tags=["Tags"],
    summary="List all tags with book counts"
)
async def get_tags(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page")
):

    pipeline = [
        {"$lookup": {
            "from": "book_tags",
            "localField": "tag_id",
            "foreignField": "tag_id",
            "as": "books"
        }},
        {"$project": {
            "_id": 0,
            "tag_id": 1,
            "tag_name": 1,
            "book_count": {"$size": "$books"}
        }},
        {"$sort": {"book_count": -1}},
        {"$skip": (page-1)*page_size},
        {"$limit": page_size}
    ]
    
    tags = await db.tags.aggregate(pipeline).to_list(length=page_size)
    total = await db.tags.count_documents({})
    
    return {"items": tags, "page": page, "page_size": page_size, "total": total}

# GET /users/{user_id}/to-read
@app.get(
    "/users/{user_id}/to-read",
    tags=["Users"],
    summary="Get user's to-read list"
)
async def get_to_read(user_id: int = Path(..., description="User ID")):
    docs = await db.to_read.find({"user_id": user_id}, {"_id": 0}).to_list(length=500)
    
    if not docs:
        return []
    
    book_ids = [d["book_id"] for d in docs]
    
    # Join with books to get details (enhancement beyond assignment requirement)
    books = await db.books.find(
        {"book_id": {"$in": book_ids}},
        {"_id": 0, "book_id": 1, "title": 1, "authors": 1, "average_rating": 1}
    ).to_list(length=len(book_ids))
    
    return books

# GET /books/{book_id}/ratings/summary
@app.get(
    "/books/{book_id}/ratings/summary",
    tags=["Ratings"],
    summary="Get ratings summary for a book"
)
async def get_ratings_summary(book_id: int = Path(..., description="Book ID")):
    book = await db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    # Get histogram of ratings
    histogram_pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {"_id": "$rating", "count": {"$sum": 1}}}
    ]
    
    histogram_results = await db.ratings.aggregate(histogram_pipeline).to_list(length=None)
    histogram = {r["_id"]: r["count"] for r in histogram_results}
    
    # Ensure all ratings 1-5 are present (fill with 0 if missing)
    histogram = {i: histogram.get(i, 0) for i in range(1, 6)}
    
    # Calculate average and total count
    avg_pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {
            "_id": None,
            "avg": {"$avg": "$rating"},
            "count": {"$sum": 1}
        }}
    ]
    
    avg_result = await db.ratings.aggregate(avg_pipeline).to_list(length=1)
    
    if not avg_result:
        return {
            "book_id": book_id,
            "avg": 0,
            "count": 0,
            "histogram": histogram
        }
    
    return {
        "book_id": book_id,
        "avg": round(avg_result[0]["avg"], 2),
        "count": int(avg_result[0]["count"]),
        "histogram": histogram
    }

# POST /ratings
@app.post(
    "/ratings",
    tags=["Ratings"],
    summary="Add or update a rating (protected)"
)
async def add_rating(
    rating: Rating,
    x_api_key: str = Header(..., description="API key for authentication")
):
    # Verify API key
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    
    # Verify book exists
    book = await db.books.find_one({"book_id": rating.book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    result = await db.ratings.update_one(
        {"user_id": rating.user_id, "book_id": rating.book_id},
        {"$set": rating.dict()},
        upsert=True
    )
    
    status_code = 201 if result.upserted_id else 200
    
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "success",
            "message": "Rating created" if result.upserted_id else "Rating updated",
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted": bool(result.upserted_id)
        }
    )

# Recommendations
@app.get(
    "/users/{user_id}/recommendations",
    tags=["Users"],
    summary="Get personalized book recommendations"
)
async def get_recommendations(
    user_id: int = Path(..., description="User ID"),
    top_k: int = Query(20, ge=1, le=100, description="Number of recommendations to return")
):
    try:
        # Get user's high ratings (4-5 stars)
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
            
            return {
                "user_id": user_id,
                "recommendations": books,
                "algorithm": "top_rated_overall",
                "reason": "User has no ratings yet"
            }
        
        user_book_ids = [r["book_id"] for r in user_ratings]
        
        # Get goodreads_book_ids for these books
        user_books = await db.books.find(
            {"book_id": {"$in": user_book_ids}},
            {"goodreads_book_id": 1, "book_id": 1, "_id": 0}
        ).to_list(length=len(user_book_ids))
        
        goodreads_ids = [b.get("goodreads_book_id", b.get("book_id")) for b in user_books if b.get("goodreads_book_id") or b.get("book_id")]
        
        if not goodreads_ids:
            # Fallback
            books = await db.books.find(
                {"average_rating": {"$gte": 4.0}},
                {"_id": 0}
            ).sort("average_rating", -1).limit(top_k).to_list(length=top_k)
            
            return {
                "user_id": user_id,
                "recommendations": books,
                "algorithm": "top_rated_overall",
                "reason": "Could not extract book tags"
            }
        
        # Find tags associated with user's favorite books
        user_tags = await db.book_tags.find(
            {"goodreads_book_id": {"$in": goodreads_ids}},
            {"tag_id": 1, "_id": 0}
        ).to_list(length=None)
        
        if not user_tags:
            # Fallback
            books = await db.books.find(
                {"average_rating": {"$gte": 4.0}},
                {"_id": 0}
            ).sort("average_rating", -1).limit(top_k).to_list(length=top_k)
            
            return {
                "user_id": user_id,
                "recommendations": books,
                "algorithm": "top_rated_overall",
                "reason": "No tags found for user's favorite books"
            }
        
        # Count tag frequencies
        tag_counts = defaultdict(int)
        for ut in user_tags:
            tag_counts[ut["tag_id"]] += 1
        
        # Get top 10 most frequent tags
        top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_tag_ids = [t[0] for t in top_tags]
        
        # Find books with these tags
        recommended_book_tags = await db.book_tags.find(
            {"tag_id": {"$in": top_tag_ids}},
            {"goodreads_book_id": 1, "_id": 0}
        ).to_list(length=None)
        
        recommended_goodreads_ids = list(set([bt["goodreads_book_id"] for bt in recommended_book_tags]))
        
        # Get book details, exclude already rated books
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
            "user_id": user_id,
            "recommendations": recommendations,
            "algorithm": "tag_based_collaborative_filtering",
            "user_favorite_tags": top_tag_ids[:5],
            "based_on_books": len(user_book_ids)
        }
    
    except Exception as e:
        # Error handling
        raise HTTPException(
            status_code=500,
            detail=f"Error generating recommendations: {str(e)}"
        )

# Custom Exception Handler
@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

# Startup Event
@app.on_event("startup")
async def startup_event():
    print(f"Goodreads API starting...")
    print(f"Database: {DB_NAME}")
    print(f"Rate limit: {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW}s")
    print(f"Docs available at: http://localhost:8000/docs")

# Shutdown Event
@app.on_event("shutdown")
async def shutdown_event():
    client.close()
    print("Goodreads API shutting down...")