"""
EVC - E-Video Cloud Backend API
FastAPI backend for movie data retrieval from OMDB API
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from contextlib import asynccontextmanager
import httpx
import os
from datetime import datetime, timedelta
from functools import lru_cache
import asyncio

# OMDB API Configuration
OMDB_API_KEY = "fc57e7f4"
OMDB_BASE_URL = "http://www.omdbapi.com/"

# In-memory cache for API responses (in production, use Redis)
cache = {}
CACHE_DURATION = timedelta(hours=1)


class OMDBClient:
    """Client for interacting with OMDB API with caching"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=10.0)
    
    async def search_movies(
        self, 
        query: str, 
        page: int = 1,
        year: Optional[int] = None,
        type: Optional[str] = None
    ) -> dict:
        """Search for movies by title"""
        cache_key = f"search:{query}:{page}:{year}:{type}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        params = {
            "apikey": self.api_key,
            "s": query,
            "page": page,
            "type": type or "movie"
        }
        if year:
            params["y"] = year
        
        try:
            response = await self.client.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Cache the response
            cache[cache_key] = (data, datetime.now())
            
            return data
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"OMDB API error: {str(e)}")
    
    async def get_movie_by_id(self, imdb_id: str, raise_on_error: bool = False) -> dict:
        """Get movie details by IMDb ID"""
        cache_key = f"movie:{imdb_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        params = {
            "apikey": self.api_key,
            "i": imdb_id
        }
        
        try:
            response = await self.client.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("Response") == "False":
                if raise_on_error:
                    raise HTTPException(status_code=404, detail=data.get("Error", "Movie not found"))
                return None
            
            # Cache the response
            cache[cache_key] = (data, datetime.now())
            
            return data
        except httpx.HTTPError as e:
            if raise_on_error:
                raise HTTPException(status_code=500, detail=f"OMDB API error: {str(e)}")
            return None
    
    async def enrich_movie_details(self, movies: List[dict]) -> List[dict]:
        """Enrich movie list with full details including ratings"""
        if not movies:
            return []
        
        # Fetch full details for each movie concurrently
        tasks = [self.get_movie_by_id(movie.get("imdbID"), raise_on_error=False) for movie in movies if movie.get("imdbID")]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        enriched_movies = []
        for i, result in enumerate(results):
            if isinstance(result, dict) and result.get("Response") != "False":
                enriched_movies.append(result)
            elif result is None or isinstance(result, Exception):
                # If fetching details fails, keep the original movie data
                if i < len(movies):
                    enriched_movies.append(movies[i])
        
        return enriched_movies
    
    async def get_popular_movies(self, limit: int = 20, enrich: bool = False) -> List[dict]:
        """Get popular movies by searching for common terms"""
        popular_queries = [
            "action", "comedy", "drama", "thriller", "sci-fi",
            "superhero", "marvel", "batman", "spider", "avengers"
        ]
        
        all_movies = []
        seen_ids = set()
        
        # Fetch movies from multiple popular queries
        tasks = [self.search_movies(query, page=1) for query in popular_queries[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, dict) and result.get("Response") == "True":
                movies = result.get("Search", [])
                for movie in movies:
                    imdb_id = movie.get("imdbID")
                    if imdb_id and imdb_id not in seen_ids:
                        seen_ids.add(imdb_id)
                        all_movies.append(movie)
                        if len(all_movies) >= limit:
                            break
            if len(all_movies) >= limit:
                break
        
        movies_list = all_movies[:limit]
        
        # Optionally enrich with full details
        if enrich:
            movies_list = await self.enrich_movie_details(movies_list)
        
        return movies_list
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


# Initialize OMDB client (will be set in lifespan)
omdb_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global omdb_client
    omdb_client = OMDBClient(OMDB_API_KEY, OMDB_BASE_URL)
    yield
    # Shutdown
    if omdb_client:
        await omdb_client.close()


app = FastAPI(
    title="EVC API",
    description="E-Video Cloud Movie API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "EVC API is running", "version": "1.0.0"}


@app.get("/api/movies/search")
async def search_movies(
    query: str = Query(..., description="Search query for movies"),
    page: int = Query(1, ge=1, description="Page number"),
    year: Optional[int] = Query(None, description="Filter by year"),
    type: Optional[str] = Query(None, description="Type: movie, series, episode")
):
    """Search for movies"""
    try:
        result = await omdb_client.search_movies(query, page, year, type)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# IMPORTANT: Specific routes must come BEFORE parameterized routes
@app.get("/api/movies/popular")
async def get_popular_movies(
    limit: int = Query(20, ge=1, le=100),
    enrich: bool = Query(False, description="Enrich with full movie details")
):
    """Get popular movies"""
    try:
        movies = await omdb_client.get_popular_movies(limit, enrich=enrich)
        return {"Response": "True", "Search": movies, "totalResults": str(len(movies))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/movies/new-releases")
async def get_new_releases(limit: int = Query(6, ge=1, le=20)):
    """Get new releases (movies from recent years) with full details"""
    current_year = datetime.now().year
    recent_years = [current_year, current_year - 1, current_year - 2]
    
    all_movies = []
    seen_ids = set()
    
    # Search for popular movies from recent years
    search_queries = ["the", "a", "action", "comedy", "drama"]
    tasks = []
    for year in recent_years:
        for query in search_queries[:2]:  # Limit queries to avoid too many requests
            tasks.append(omdb_client.search_movies(query, page=1, year=year))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, dict) and result.get("Response") == "True":
            movies = result.get("Search", [])
            for movie in movies:
                imdb_id = movie.get("imdbID")
                if imdb_id and imdb_id not in seen_ids:
                    seen_ids.add(imdb_id)
                    all_movies.append(movie)
                    if len(all_movies) >= limit * 2:  # Get more to have options
                        break
        if len(all_movies) >= limit * 2:
            break
    
    # Take first N movies and enrich with full details
    movies_list = all_movies[:limit]
    enriched_movies = await omdb_client.enrich_movie_details(movies_list)
    
    return {"Response": "True", "Search": enriched_movies, "totalResults": str(len(enriched_movies))}


@app.get("/api/movies/{imdb_id}")
async def get_movie(imdb_id: str):
    """Get movie details by IMDb ID"""
    try:
        result = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=True)
        if result is None:
            raise HTTPException(status_code=404, detail="Movie not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

