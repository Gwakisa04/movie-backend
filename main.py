"""
EVC - E-Video Cloud Backend API
FastAPI backend for movie data retrieval from OMDB and TMDB APIs
Combines results from both APIs for maximum movie coverage
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
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "fc57e7f4")
OMDB_BASE_URL = "http://www.omdbapi.com/"

# TMDB API Configuration
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "12bd7697a8d16869fabe58f6646611bd")
TMDB_ACCESS_TOKEN = os.environ.get("TMDB_ACCESS_TOKEN", "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxMmJkNzY5N2E4ZDE2ODY5ZmFiZTU4ZjY2NDY2MTFiZCIsIm5iZiI6MTc2Njg2MzEwNC40NTUwMDAyLCJzdWIiOiI2OTUwMzEwMGMxY2U4NjJlMGUyZmJlZDciLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.E0mtTw_3_zb_v1ZC6hrNUQPRQPXuDhrMpFw1lrz8PWM")
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"

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


class TMDBClient:
    """Client for interacting with TMDB API with caching"""
    
    def __init__(self, access_token: str, base_url: str):
        self.access_token = access_token
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "Authorization": f"Bearer {access_token}",
                "accept": "application/json"
            }
        )
    
    def _normalize_tmdb_to_omdb_format(self, tmdb_item: dict, item_type: str = "movie") -> dict:
        """Convert TMDB movie/TV format to OMDB-like format for consistency"""
        # Handle both movies and TV shows
        title = tmdb_item.get("title") or tmdb_item.get("name", "")
        release_date = tmdb_item.get("release_date") or tmdb_item.get("first_air_date", "")
        
        return {
            "Title": title,
            "Year": str(release_date)[:4] if release_date else "N/A",
            "imdbID": f"tmdb_{tmdb_item.get('id')}",  # Prefix to avoid conflicts
            "Type": item_type,
            "Poster": f"{TMDB_IMAGE_BASE_URL}{tmdb_item.get('poster_path', '')}" if tmdb_item.get("poster_path") else "N/A",
            "tmdb_id": tmdb_item.get("id"),
            "tmdb_vote_average": tmdb_item.get("vote_average"),
            "tmdb_vote_count": tmdb_item.get("vote_count"),
            "overview": tmdb_item.get("overview", ""),
            "release_date": release_date,
            "popularity": tmdb_item.get("popularity", 0),
            "source": "tmdb"
        }
    
    async def search_movies(
        self,
        query: str,
        page: int = 1,
        year: Optional[int] = None,
        content_type: Optional[str] = None
    ) -> dict:
        """Search for movies/TV shows by title"""
        # Map content types: "series" -> "tv", "anime" -> search with anime genre
        search_type = "movie"
        if content_type == "series":
            search_type = "tv"
        elif content_type == "anime":
            # For anime, we'll search TV shows with anime genre filter
            search_type = "tv"
        
        cache_key = f"tmdb_search:{query}:{page}:{year}:{content_type}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        params = {
            "query": query,
            "page": page,
            "include_adult": "false"
        }
        if year:
            params["year"] = year
        
        try:
            url = f"{self.base_url}/search/{search_type}"
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Normalize to OMDB-like format
            normalized_results = []
            for item in data.get("results", []):
                item_type = "series" if search_type == "tv" else "movie"
                if content_type == "anime":
                    item_type = "anime"
                normalized_results.append(self._normalize_tmdb_to_omdb_format(item, item_type))
            
            result = {
                "Response": "True",
                "Search": normalized_results,
                "totalResults": str(data.get("total_results", 0))
            }
            
            # Cache the response
            cache[cache_key] = (result, datetime.now())
            
            return result
        except httpx.HTTPError as e:
            return {"Response": "False", "Error": f"TMDB API error: {str(e)}", "Search": []}
    
    async def get_popular_tv_shows(self, limit: int = 20, page: int = 1) -> List[dict]:
        """Get popular TV shows from TMDB"""
        cache_key = f"tmdb_popular_tv:{page}:{limit}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data.get("results", [])[:limit]
        
        try:
            url = f"{self.base_url}/tv/popular"
            params = {"page": page}
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Normalize results
            normalized_results = []
            for show in data.get("results", [])[:limit]:
                normalized_results.append(self._normalize_tmdb_to_omdb_format(show, "series"))
            
            # Cache the response
            cache[cache_key] = ({"results": normalized_results}, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            return []
    
    async def get_anime_shows(self, limit: int = 20, page: int = 1) -> List[dict]:
        """Get anime shows from TMDB (using anime genre)"""
        cache_key = f"tmdb_anime:{page}:{limit}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data.get("results", [])[:limit]
        
        try:
            # TMDB anime genre ID is 16
            url = f"{self.base_url}/discover/tv"
            params = {
                "page": page,
                "with_genres": "16",  # Anime genre
                "sort_by": "popularity.desc"
            }
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Normalize results
            normalized_results = []
            for show in data.get("results", [])[:limit]:
                normalized_results.append(self._normalize_tmdb_to_omdb_format(show, "anime"))
            
            # Cache the response
            cache[cache_key] = ({"results": normalized_results}, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            return []
    
    async def get_popular_movies(self, limit: int = 20, page: int = 1) -> List[dict]:
        """Get popular movies from TMDB"""
        cache_key = f"tmdb_popular:{page}:{limit}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data.get("results", [])[:limit]
        
        try:
            url = f"{self.base_url}/movie/popular"
            params = {"page": page}
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Normalize results
            normalized_results = []
            for movie in data.get("results", [])[:limit]:
                normalized_results.append(self._normalize_tmdb_to_omdb_format(movie))
            
            # Cache the response
            cache[cache_key] = ({"results": normalized_results}, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            return []
    
    async def get_now_playing_movies(self, limit: int = 20, page: int = 1) -> List[dict]:
        """Get now playing movies from TMDB"""
        cache_key = f"tmdb_now_playing:{page}:{limit}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data.get("results", [])[:limit]
        
        try:
            url = f"{self.base_url}/movie/now_playing"
            params = {"page": page}
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Normalize results
            normalized_results = []
            for movie in data.get("results", [])[:limit]:
                normalized_results.append(self._normalize_tmdb_to_omdb_format(movie))
            
            # Cache the response
            cache[cache_key] = ({"results": normalized_results}, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            return []
    
    async def get_movie_by_id(self, tmdb_id: int, raise_on_error: bool = False) -> dict:
        """Get movie details by TMDB ID"""
        cache_key = f"tmdb_movie:{tmdb_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/movie/{tmdb_id}"
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Normalize to OMDB-like format
            normalized = self._normalize_tmdb_to_omdb_format(data)
            normalized["Response"] = "True"
            
            # Cache the response
            cache[cache_key] = (normalized, datetime.now())
            
            return normalized
        except httpx.HTTPError as e:
            if raise_on_error:
                raise HTTPException(status_code=500, detail=f"TMDB API error: {str(e)}")
            return None
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


def merge_movie_results(omdb_movies: List[dict], tmdb_movies: List[dict], limit: int) -> List[dict]:
    """Merge and deduplicate movie results from both APIs"""
    seen_titles = set()
    merged = []
    
    # Add OMDB movies first (they have IMDb IDs which are more reliable)
    for movie in omdb_movies:
        title_key = movie.get("Title", "").lower().strip()
        if title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
            merged.append(movie)
            if len(merged) >= limit:
                return merged[:limit]
    
    # Add TMDB movies that aren't duplicates
    for movie in tmdb_movies:
        title_key = movie.get("Title", "").lower().strip()
        if title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
            merged.append(movie)
            if len(merged) >= limit:
                return merged[:limit]
    
    return merged[:limit]


# Initialize clients (will be set in lifespan)
omdb_client = None
tmdb_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global omdb_client, tmdb_client
    omdb_client = OMDBClient(OMDB_API_KEY, OMDB_BASE_URL)
    tmdb_client = TMDBClient(TMDB_ACCESS_TOKEN, TMDB_BASE_URL)
    yield
    # Shutdown
    if omdb_client:
        await omdb_client.close()
    if tmdb_client:
        await tmdb_client.close()


app = FastAPI(
    title="EVC API",
    description="E-Video Cloud Movie API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware to allow frontend requests
# In production, update allow_origins with your frontend URL
cors_origins = os.environ.get(
    "CORS_ORIGINS", 
    "http://localhost:3000,http://localhost:5173"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
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
    type: Optional[str] = Query(None, description="Type: movie, series, episode, anime")
):
    """Search for movies/TV series/Anime from both OMDB and TMDB"""
    try:
        # Map type for OMDB (series -> series, anime -> series for OMDB)
        omdb_type = type
        if type == "anime":
            omdb_type = "series"  # OMDB doesn't have anime type, use series
        
        # Search both APIs concurrently
        omdb_task = omdb_client.search_movies(query, page, year, omdb_type)
        tmdb_task = tmdb_client.search_movies(query, page, year, type)
        
        omdb_result, tmdb_result = await asyncio.gather(
            omdb_task,
            tmdb_task,
            return_exceptions=True
        )
        
        # Extract movies from both results
        omdb_movies = []
        tmdb_movies = []
        
        if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
            omdb_movies = omdb_result.get("Search", [])
            # Filter by type if specified
            if type:
                omdb_movies = [m for m in omdb_movies if m.get("Type", "").lower() == type.lower()]
        
        if isinstance(tmdb_result, dict) and tmdb_result.get("Response") == "True":
            tmdb_movies = tmdb_result.get("Search", [])
        
        # Merge results (limit to 20 per page)
        merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit=20)
        
        return {
            "Response": "True",
            "Search": merged_movies,
            "totalResults": str(len(merged_movies))
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# IMPORTANT: Specific routes must come BEFORE parameterized routes
@app.get("/api/movies/popular")
async def get_popular_movies(
    limit: int = Query(20, ge=1, le=100),
    enrich: bool = Query(False, description="Enrich with full movie details"),
    type: Optional[str] = Query(None, description="Type: movie, series, anime")
):
    """Get popular movies/TV series/Anime from both OMDB and TMDB"""
    try:
        if type == "series":
            # Get TV series
            omdb_task = omdb_client.search_movies("the", page=1, type="series")
            tmdb_task = tmdb_client.get_popular_tv_shows(limit)
            
            omdb_result, tmdb_movies = await asyncio.gather(
                omdb_task,
                tmdb_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 2]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit)
            
        elif type == "anime":
            # Get anime
            omdb_task = omdb_client.search_movies("anime", page=1, type="series")
            tmdb_task = tmdb_client.get_anime_shows(limit)
            
            omdb_result, tmdb_movies = await asyncio.gather(
                omdb_task,
                tmdb_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 2]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit)
            
        else:
            # Get movies (default)
            omdb_task = omdb_client.get_popular_movies(limit // 2, enrich=enrich)
            tmdb_task = tmdb_client.get_popular_movies(limit // 2)
            
            omdb_movies, tmdb_movies = await asyncio.gather(
                omdb_task,
                tmdb_task,
                return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(omdb_movies, Exception):
                omdb_movies = []
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            # Merge results
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit)
        
        return {"Response": "True", "Search": merged_movies, "totalResults": str(len(merged_movies))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/movies/new-releases")
async def get_new_releases(
    limit: int = Query(6, ge=1, le=20),
    type: Optional[str] = Query(None, description="Type: movie, series, anime")
):
    """Get new releases from both OMDB and TMDB"""
    try:
        if type == "series":
            # For TV series, get popular TV shows as "new releases"
            tmdb_task = tmdb_client.get_popular_tv_shows(limit)
            omdb_task = omdb_client.search_movies("the", page=1, type="series")
            
            tmdb_movies, omdb_result = await asyncio.gather(
                tmdb_task,
                omdb_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 2]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit)
            
        elif type == "anime":
            # For anime, get anime shows
            tmdb_task = tmdb_client.get_anime_shows(limit)
            omdb_task = omdb_client.search_movies("anime", page=1, type="series")
            
            tmdb_movies, omdb_result = await asyncio.gather(
                tmdb_task,
                omdb_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 2]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit)
            
        else:
            # For movies (default), fetch from TMDB's "now playing" endpoint
            tmdb_task = tmdb_client.get_now_playing_movies(limit)
            
            # Also search OMDB for recent movies
            current_year = datetime.now().year
            omdb_tasks = [
                omdb_client.search_movies("the", page=1, year=current_year),
                omdb_client.search_movies("action", page=1, year=current_year - 1)
            ]
            
            tmdb_movies, *omdb_results = await asyncio.gather(
                tmdb_task,
                *omdb_tasks,
                return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            # Collect OMDB movies
            omdb_movies = []
            for result in omdb_results:
                if isinstance(result, dict) and result.get("Response") == "True":
                    omdb_movies.extend(result.get("Search", []))
            
            # Merge results
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, limit)
            
            # Enrich with full details from OMDB if available
            merged_movies = await omdb_client.enrich_movie_details(merged_movies)
        
        return {"Response": "True", "Search": merged_movies, "totalResults": str(len(merged_movies))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

