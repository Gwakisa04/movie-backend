"""
EVC - E-Video Cloud Backend API
FastAPI backend for movie data retrieval from OMDB, TMDB, TVMaze, WatchMode, and YouTube APIs
Combines results from all five APIs for maximum movie coverage with trailers, streaming links, and music videos
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

# TVMaze API Configuration (Free, no API key required)
TVMAZE_BASE_URL = "https://api.tvmaze.com"

# WatchMode API Configuration (for trailers and streaming platform links)
WATCHMODE_API_KEY = os.environ.get("WATCHMODE_API_KEY", "YH05AmtNmPZLFuvBziKL2XvKJmNJHsCLEvHy4EXx")
WATCHMODE_BASE_URL = "https://api.watchmode.com/v1"

# YouTube Data API Configuration (for music videos and trailers)
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyCvjc895h-fmSW3ZMn-jy3wWtHk4wH3fx8")
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"

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


class TVMazeClient:
    """Client for interacting with TVMaze API with caching (Free, no API key required)"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "accept": "application/json",
                "User-Agent": "EVC-API/1.0"  # Recommended by TVMaze
            }
        )
    
    def _normalize_tvmaze_to_omdb_format(self, tvmaze_item: dict, item_type: str = "series") -> dict:
        """Convert TVMaze show format to OMDB-like format for consistency"""
        show = tvmaze_item.get("show", tvmaze_item)  # Search results wrap in "show" key
        
        # Extract image URL
        image_url = "N/A"
        if show.get("image"):
            image_url = show["image"].get("medium") or show["image"].get("original", "N/A")
        
        # Extract year from premiered date
        year = "N/A"
        if show.get("premiered"):
            year = str(show["premiered"])[:4]
        
        # Get IMDb ID if available
        imdb_id = show.get("externals", {}).get("imdb", "")
        if not imdb_id:
            imdb_id = f"tvmaze_{show.get('id')}"  # Use TVMaze ID as fallback
        
        return {
            "Title": show.get("name", ""),
            "Year": year,
            "imdbID": imdb_id,
            "Type": item_type,
            "Poster": image_url,
            "tvmaze_id": show.get("id"),
            "tvmaze_rating": show.get("rating", {}).get("average"),
            "overview": show.get("summary", "").replace("<p>", "").replace("</p>", "").replace("<b>", "").replace("</b>", "") if show.get("summary") else "",
            "premiered": show.get("premiered"),
            "genres": show.get("genres", []),
            "runtime": show.get("runtime"),
            "status": show.get("status"),
            "source": "tvmaze"
        }
    
    async def search_shows(
        self,
        query: str,
        page: int = 1,
        year: Optional[int] = None,
        content_type: Optional[str] = None
    ) -> dict:
        """Search for TV shows by title"""
        # TVMaze is primarily for TV shows, but we can use it for series/anime
        if content_type == "movie":
            # TVMaze doesn't have movies, skip for movie searches
            return {"Response": "False", "Error": "TVMaze doesn't support movies", "Search": []}
        
        cache_key = f"tvmaze_search:{query}:{page}:{year}:{content_type}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        params = {"q": query}
        
        try:
            url = f"{self.base_url}/search/shows"
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Normalize to OMDB-like format
            normalized_results = []
            for item in data:
                # Determine item type
                item_type = "series"
                if content_type == "anime":
                    # Check if it's anime based on genres
                    show = item.get("show", item)
                    genres = show.get("genres", [])
                    if "Anime" in genres or "Animation" in genres:
                        item_type = "anime"
                
                # Filter by year if specified
                if year:
                    show = item.get("show", item)
                    premiered = show.get("premiered")
                    if premiered:
                        show_year = int(str(premiered)[:4])
                        if show_year != year:
                            continue
                
                normalized_results.append(self._normalize_tvmaze_to_omdb_format(item, item_type))
            
            # Apply pagination (TVMaze doesn't support pagination, so we do it manually)
            start_idx = (page - 1) * 10
            end_idx = start_idx + 10
            paginated_results = normalized_results[start_idx:end_idx]
            
            result = {
                "Response": "True",
                "Search": paginated_results,
                "totalResults": str(len(normalized_results))
            }
            
            # Cache the response
            cache[cache_key] = (result, datetime.now())
            
            return result
        except httpx.HTTPError as e:
            return {"Response": "False", "Error": f"TVMaze API error: {str(e)}", "Search": []}
    
    async def get_popular_shows(self, limit: int = 20, content_type: Optional[str] = None) -> List[dict]:
        """Get popular TV shows from TVMaze by searching popular terms"""
        if content_type == "movie":
            return []  # TVMaze doesn't have movies
        
        cache_key = f"tvmaze_popular:{limit}:{content_type}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data.get("results", [])[:limit]
        
        # Search for popular TV show terms
        popular_queries = [
            "the", "game", "house", "love", "man", "woman", "city", "night", "day",
            "breaking", "walking", "stranger", "crown", "office", "friends"
        ]
        
        all_shows = []
        seen_ids = set()
        
        # Fetch shows from multiple popular queries
        tasks = [self.search_shows(query, page=1, content_type=content_type) for query in popular_queries[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, dict) and result.get("Response") == "True":
                shows = result.get("Search", [])
                for show in shows:
                    tvmaze_id = show.get("tvmaze_id")
                    if tvmaze_id and tvmaze_id not in seen_ids:
                        seen_ids.add(tvmaze_id)
                        all_shows.append(show)
                        if len(all_shows) >= limit:
                            break
            if len(all_shows) >= limit:
                break
        
        shows_list = all_shows[:limit]
        
        # Cache the response
        cache[cache_key] = ({"results": shows_list}, datetime.now())
        
        return shows_list
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class WatchModeClient:
    """Client for interacting with WatchMode API for trailers and streaming platform links"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "accept": "application/json"
            }
        )
    
    async def search_titles(self, query: str, search_type: str = "movie") -> List[dict]:
        """Search for movies/TV shows by title"""
        cache_key = f"watchmode_search:{query}:{search_type}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/search/"
            params = {
                "apiKey": self.api_key,
                "search_field": "name",
                "search_value": query,
                "types": search_type  # movie, tv_series, etc.
            }
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = data.get("title_results", [])
            
            # Cache the response
            cache[cache_key] = (results, datetime.now())
            
            return results
        except httpx.HTTPError as e:
            return []
    
    async def get_title_details(self, title_id: int) -> Optional[dict]:
        """Get detailed information about a title including trailer"""
        cache_key = f"watchmode_details:{title_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/title/{title_id}/details/"
            params = {"apiKey": self.api_key}
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Cache the response
            cache[cache_key] = (data, datetime.now())
            
            return data
        except httpx.HTTPError as e:
            return None
    
    async def get_title_sources(self, title_id: int) -> List[dict]:
        """Get streaming platform sources for a title"""
        cache_key = f"watchmode_sources:{title_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/title/{title_id}/sources/"
            params = {"apiKey": self.api_key}
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            sources = data if isinstance(data, list) else []
            
            # Cache the response (shorter cache for sources as they change frequently)
            cache[cache_key] = (sources, datetime.now())
            
            return sources
        except httpx.HTTPError as e:
            return []
    
    async def enrich_movie_with_watchmode(self, movie: dict) -> dict:
        """Enrich a movie object with WatchMode data (trailer and streaming sources)"""
        # Try to find the movie in WatchMode by title
        title = movie.get("Title", "")
        if not title:
            return movie
        
        # Search for the title
        watchmode_results = await self.search_titles(title)
        
        if not watchmode_results:
            return movie
        
        # Use the first result (best match)
        watchmode_title = watchmode_results[0]
        title_id = watchmode_title.get("id")
        
        if not title_id:
            return movie
        
        # Get details and sources concurrently
        details_task = self.get_title_details(title_id)
        sources_task = self.get_title_sources(title_id)
        
        details, sources = await asyncio.gather(
            details_task,
            sources_task,
            return_exceptions=True
        )
        
        # Add WatchMode data to movie
        if isinstance(details, dict):
            movie["trailer"] = details.get("trailer")
            movie["watchmode_id"] = title_id
            movie["watchmode_imdb_id"] = details.get("imdb_id")
            movie["watchmode_tmdb_id"] = details.get("tmdb_id")
            movie["watchmode_tmdb_type"] = details.get("tmdb_type")
        
        if isinstance(sources, list):
            # Group sources by type
            streaming_sources = []
            for source in sources:
                streaming_sources.append({
                    "name": source.get("name"),
                    "type": source.get("type"),  # free, subscription, rental, buy
                    "web_url": source.get("web_url"),
                    "ios_url": source.get("ios_url"),
                    "android_url": source.get("android_url"),
                    "format": source.get("format"),  # sd, hd, 4k
                    "price": source.get("price"),
                    "season": source.get("season"),
                    "episode": source.get("episode")
                })
            movie["streaming_sources"] = streaming_sources
        
        return movie
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class YouTubeClient:
    """Client for interacting with YouTube Data API v3 for music videos and trailers"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "accept": "application/json"
            }
        )
    
    async def search_videos(
        self, 
        query: str, 
        max_results: int = 10,
        video_category: Optional[str] = None,
        video_type: str = "video"
    ) -> List[dict]:
        """Search for videos on YouTube"""
        cache_key = f"youtube_search:{query}:{max_results}:{video_category}:{video_type}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/search"
            params = {
                "key": self.api_key,
                "part": "snippet",
                "q": query,
                "type": video_type,
                "maxResults": max_results,
                "order": "relevance"
            }
            
            # Add video category for music videos (categoryId=10)
            if video_category == "music":
                params["videoCategoryId"] = "10"
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            videos = []
            for item in data.get("items", []):
                video_id = item.get("id", {}).get("videoId")
                snippet = item.get("snippet", {})
                
                videos.append({
                    "video_id": video_id,
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url") or snippet.get("thumbnails", {}).get("medium", {}).get("url"),
                    "channel_title": snippet.get("channelTitle"),
                    "published_at": snippet.get("publishedAt"),
                    "url": f"https://www.youtube.com/watch?v={video_id}" if video_id else None,
                    "embed_url": f"https://www.youtube.com/embed/{video_id}" if video_id else None
                })
            
            # Cache the response
            cache[cache_key] = (videos, datetime.now())
            
            return videos
        except httpx.HTTPError as e:
            return []
    
    async def search_music_videos(self, query: str, max_results: int = 10) -> List[dict]:
        """Search specifically for music videos"""
        return await self.search_videos(query, max_results, video_category="music")
    
    async def search_trailers(self, movie_title: str, year: Optional[str] = None, max_results: int = 5) -> List[dict]:
        """Search for movie trailers on YouTube"""
        query = f"{movie_title} trailer"
        if year:
            query += f" {year}"
        
        return await self.search_videos(query, max_results, video_type="video")
    
    async def enrich_movie_with_youtube(self, movie: dict) -> dict:
        """Enrich a movie object with YouTube videos (trailers and music)"""
        title = movie.get("Title", "")
        year = movie.get("Year", "")
        
        if not title:
            return movie
        
        # Search for trailers and music videos concurrently
        trailer_query = f"{title} trailer {year}" if year else f"{title} trailer"
        music_query = f"{title} soundtrack {year}" if year else f"{title} soundtrack"
        
        trailers_task = self.search_trailers(title, year, max_results=3)
        music_task = self.search_music_videos(music_query, max_results=5)
        
        trailers, music_videos = await asyncio.gather(
            trailers_task,
            music_task,
            return_exceptions=True
        )
        
        # Add YouTube data to movie
        if isinstance(trailers, list) and trailers:
            movie["youtube_trailers"] = trailers
            # Set primary trailer URL if not already set from WatchMode
            if not movie.get("trailer") and trailers:
                movie["trailer"] = trailers[0].get("url")
        
        if isinstance(music_videos, list) and music_videos:
            movie["youtube_music_videos"] = music_videos
        
        return movie
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


def merge_movie_results(omdb_movies: List[dict], tmdb_movies: List[dict], tvmaze_shows: List[dict] = None, limit: int = 20) -> List[dict]:
    """Merge and deduplicate movie results from all three APIs"""
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
    
    # Add TVMaze shows that aren't duplicates (if provided)
    if tvmaze_shows:
        for show in tvmaze_shows:
            title_key = show.get("Title", "").lower().strip()
            if title_key and title_key not in seen_titles:
                seen_titles.add(title_key)
                merged.append(show)
                if len(merged) >= limit:
                    return merged[:limit]
    
    return merged[:limit]


# Initialize clients (will be set in lifespan)
omdb_client = None
tmdb_client = None
tvmaze_client = None
watchmode_client = None
youtube_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global omdb_client, tmdb_client, tvmaze_client, watchmode_client, youtube_client
    omdb_client = OMDBClient(OMDB_API_KEY, OMDB_BASE_URL)
    tmdb_client = TMDBClient(TMDB_ACCESS_TOKEN, TMDB_BASE_URL)
    tvmaze_client = TVMazeClient(TVMAZE_BASE_URL)
    watchmode_client = WatchModeClient(WATCHMODE_API_KEY, WATCHMODE_BASE_URL)
    youtube_client = YouTubeClient(YOUTUBE_API_KEY, YOUTUBE_BASE_URL)
    yield
    # Shutdown
    if omdb_client:
        await omdb_client.close()
    if tmdb_client:
        await tmdb_client.close()
    if tvmaze_client:
        await tvmaze_client.close()
    if watchmode_client:
        await watchmode_client.close()
    if youtube_client:
        await youtube_client.close()


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
    """Search for movies/TV series/Anime from OMDB, TMDB, and TVMaze"""
    try:
        # Map type for OMDB (series -> series, anime -> series for OMDB)
        omdb_type = type
        if type == "anime":
            omdb_type = "series"  # OMDB doesn't have anime type, use series
        
        # Search all three APIs concurrently
        omdb_task = omdb_client.search_movies(query, page, year, omdb_type)
        tmdb_task = tmdb_client.search_movies(query, page, year, type)
        # Only search TVMaze for series/anime (not movies)
        if type != "movie":
            tvmaze_task = tvmaze_client.search_shows(query, page, year, type)
            omdb_result, tmdb_result, tvmaze_result = await asyncio.gather(
                omdb_task,
                tmdb_task,
                tvmaze_task,
                return_exceptions=True
            )
        else:
            omdb_result, tmdb_result = await asyncio.gather(
                omdb_task,
                tmdb_task,
                return_exceptions=True
            )
            tvmaze_result = {"Response": "False", "Search": []}
        
        # Extract movies from all results
        omdb_movies = []
        tmdb_movies = []
        tvmaze_shows = []
        
        if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
            omdb_movies = omdb_result.get("Search", [])
            # Filter by type if specified
            if type:
                omdb_movies = [m for m in omdb_movies if m.get("Type", "").lower() == type.lower()]
        
        if isinstance(tmdb_result, dict) and tmdb_result.get("Response") == "True":
            tmdb_movies = tmdb_result.get("Search", [])
        
        if isinstance(tvmaze_result, dict) and tvmaze_result.get("Response") == "True":
            tvmaze_shows = tvmaze_result.get("Search", [])
        
        # Merge results (limit to 20 per page)
        merged_movies = merge_movie_results(omdb_movies, tmdb_movies, tvmaze_shows, limit=20)
        
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
    """Get popular movies/TV series/Anime from OMDB, TMDB, and TVMaze"""
    try:
        if type == "series":
            # Get TV series
            omdb_task = omdb_client.search_movies("the", page=1, type="series")
            tmdb_task = tmdb_client.get_popular_tv_shows(limit)
            tvmaze_task = tvmaze_client.get_popular_shows(limit, content_type="series")
            
            omdb_result, tmdb_movies, tvmaze_shows = await asyncio.gather(
                omdb_task,
                tmdb_task,
                tvmaze_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 3]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            if isinstance(tvmaze_shows, Exception):
                tvmaze_shows = []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, tvmaze_shows, limit)
            
        elif type == "anime":
            # Get anime
            omdb_task = omdb_client.search_movies("anime", page=1, type="series")
            tmdb_task = tmdb_client.get_anime_shows(limit)
            tvmaze_task = tvmaze_client.get_popular_shows(limit, content_type="anime")
            
            omdb_result, tmdb_movies, tvmaze_shows = await asyncio.gather(
                omdb_task,
                tmdb_task,
                tvmaze_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 3]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            if isinstance(tvmaze_shows, Exception):
                tvmaze_shows = []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, tvmaze_shows, limit)
            
        else:
            # Get movies (default) - TVMaze doesn't have movies, so skip it
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
            
            # Merge results (no TVMaze for movies)
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, None, limit)
        
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
async def get_movie(
    imdb_id: str, 
    include_watchmode: bool = Query(True, description="Include WatchMode data (trailers and streaming sources)"),
    include_youtube: bool = Query(True, description="Include YouTube data (trailers and music videos)")
):
    """Get movie details by IMDb ID with optional WatchMode and YouTube enrichment"""
    try:
        result = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=True)
        if result is None:
            raise HTTPException(status_code=404, detail="Movie not found")
        
        # Enrich with WatchMode data if requested
        if include_watchmode:
            try:
                result = await watchmode_client.enrich_movie_with_watchmode(result)
            except Exception as e:
                # If WatchMode fails, continue without it
                print(f"WatchMode enrichment failed: {str(e)}")
        
        # Enrich with YouTube data if requested
        if include_youtube:
            try:
                result = await youtube_client.enrich_movie_with_youtube(result)
            except Exception as e:
                # If YouTube fails, continue without it
                print(f"YouTube enrichment failed: {str(e)}")
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/movies/{imdb_id}/streaming")
async def get_movie_streaming(imdb_id: str):
    """Get streaming platform sources for a movie by IMDb ID"""
    try:
        # First get the movie to get the title
        movie = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=True)
        if movie is None:
            raise HTTPException(status_code=404, detail="Movie not found")
        
        # Search for the movie in WatchMode
        title = movie.get("Title", "")
        if not title:
            return {"streaming_sources": [], "trailer": None}
        
        watchmode_results = await watchmode_client.search_titles(title)
        if not watchmode_results:
            return {"streaming_sources": [], "trailer": None}
        
        # Get details and sources
        title_id = watchmode_results[0].get("id")
        if not title_id:
            return {"streaming_sources": [], "trailer": None}
        
        details_task = watchmode_client.get_title_details(title_id)
        sources_task = watchmode_client.get_title_sources(title_id)
        
        details, sources = await asyncio.gather(
            details_task,
            sources_task,
            return_exceptions=True
        )
        
        result = {
            "streaming_sources": [],
            "trailer": None
        }
        
        if isinstance(details, dict):
            result["trailer"] = details.get("trailer")
        
        if isinstance(sources, list):
            result["streaming_sources"] = sources
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/movies/{imdb_id}/youtube")
async def get_movie_youtube(imdb_id: str, include_music: bool = Query(True, description="Include music videos")):
    """Get YouTube videos (trailers and music) for a movie by IMDb ID"""
    try:
        # First get the movie to get the title
        movie = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=True)
        if movie is None:
            raise HTTPException(status_code=404, detail="Movie not found")
        
        title = movie.get("Title", "")
        year = movie.get("Year", "")
        
        if not title:
            return {"youtube_trailers": [], "youtube_music_videos": []}
        
        # Search for trailers and optionally music videos
        trailers_task = youtube_client.search_trailers(title, year, max_results=5)
        
        if include_music:
            music_query = f"{title} soundtrack {year}" if year else f"{title} soundtrack"
            music_task = youtube_client.search_music_videos(music_query, max_results=10)
            trailers, music_videos = await asyncio.gather(
                trailers_task,
                music_task,
                return_exceptions=True
            )
        else:
            trailers = await trailers_task
            music_videos = []
        
        result = {
            "youtube_trailers": [],
            "youtube_music_videos": []
        }
        
        if isinstance(trailers, list):
            result["youtube_trailers"] = trailers
        
        if isinstance(music_videos, list):
            result["youtube_music_videos"] = music_videos
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

