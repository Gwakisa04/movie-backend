"""
EVC - E-Video Cloud Backend API
FastAPI backend for movie data retrieval from OMDB, TMDB, TVMaze, WatchMode, YouTube, and AniList APIs
Combines results from all six APIs for maximum movie/anime/manga coverage with trailers, streaming links, and music videos
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
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w780"  # Higher quality images

# TVMaze API Configuration (Free, no API key required)
TVMAZE_BASE_URL = "https://api.tvmaze.com"

# WatchMode API Configuration (for trailers and streaming platform links)
WATCHMODE_API_KEY = os.environ.get("WATCHMODE_API_KEY", "YH05AmtNmPZLFuvBziKL2XvKJmNJHsCLEvHy4EXx")
WATCHMODE_BASE_URL = "https://api.watchmode.com/v1"

# YouTube Data API Configuration (for music videos and trailers)
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyCvjc895h-fmSW3ZMn-jy3wWtHk4wH3fx8")
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"

# AniList GraphQL API Configuration (for anime and manga - Free, no API key required)
ANILIST_BASE_URL = "https://graphql.anilist.co"

# Kitsu API Configuration (for manga - Free, no API key required)
KITSU_BASE_URL = "https://kitsu.io/api/edge"

# Project Gutenberg (Gutendex) API Configuration (for books - Free, no API key required)
GUTENDEX_BASE_URL = "https://gutendex.com/books/"

# JustWatch API Configuration (for direct watch links and streaming availability)
JUSTWATCH_BASE_URL = "https://apis.justwatch.com"
JUSTWATCH_COUNTRY = "US"  # Default country, can be made configurable

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
    
    async def get_person_details(self, person_id: int) -> Optional[dict]:
        """Get person (actor) details by TMDB ID"""
        cache_key = f"tmdb_person:{person_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/person/{person_id}"
            params = {"append_to_response": "images,combined_credits"}
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Cache the response
            cache[cache_key] = (data, datetime.now())
            
            return data
        except httpx.HTTPError as e:
            return None
    
    async def search_people(self, query: str, page: int = 1) -> List[dict]:
        """Search for people (actors) by name"""
        cache_key = f"tmdb_people_search:{query}:{page}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/search/person"
            params = {
                "query": query,
                "page": page
            }
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = data.get("results", [])
            
            # Cache the response
            cache[cache_key] = (results, datetime.now())
            
            return results
        except httpx.HTTPError as e:
            return []
    
    async def get_popular_people(self, page: int = 1) -> List[dict]:
        """Get popular people (actors)"""
        cache_key = f"tmdb_popular_people:{page}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/person/popular"
            params = {"page": page}
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = data.get("results", [])
            
            # Cache the response
            cache[cache_key] = (results, datetime.now())
            
            return results
        except httpx.HTTPError as e:
            return []
    
    async def get_watch_providers(self, tmdb_id: int, is_tv: bool = False, country: str = "US") -> Optional[dict]:
        """Get watch providers (streaming availability) for a movie or TV show"""
        cache_key = f"tmdb_watch_providers:{tmdb_id}:{is_tv}:{country}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            content_type = "tv" if is_tv else "movie"
            url = f"{self.base_url}/{content_type}/{tmdb_id}/watch/providers"
            params = {}
            if country:
                params["watch_region"] = country
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Cache the response
            cache[cache_key] = (data, datetime.now())
            
            return data
        except httpx.HTTPError as e:
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
    
    async def get_show_by_id(self, show_id: int, raise_on_error: bool = False) -> Optional[dict]:
        """Get TV show details by TVMaze ID"""
        cache_key = f"tvmaze_show:{show_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/shows/{show_id}"
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Normalize to OMDB-like format
            normalized = self._normalize_tvmaze_to_omdb_format(data, item_type="series")
            normalized["Response"] = "True"
            
            # Cache the response
            cache[cache_key] = (normalized, datetime.now())
            
            return normalized
        except httpx.HTTPError as e:
            if raise_on_error:
                raise HTTPException(status_code=404, detail=f"TVMaze show not found: {str(e)}")
            return None
    
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
            trailer_url = details.get("trailer")
            # Convert trailer URL to embed format if it's a YouTube URL
            if trailer_url and "youtube.com" in trailer_url:
                # Extract video ID and convert to embed URL
                import re
                video_match = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/)([^&\s]+)', trailer_url)
                if video_match:
                    video_id = video_match.group(1).split('&')[0].split('?')[0]
                    trailer_url = f"https://www.youtube.com/embed/{video_id}"
            movie["trailer"] = trailer_url
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
    
    def _convert_to_embed_url(self, url: str) -> Optional[str]:
        """Convert any YouTube URL to embed format"""
        if not url:
            return None
        
        # If already embed URL, return as is
        if "youtube.com/embed/" in url:
            return url
        
        # Extract video ID from various formats
        import re
        
        # Format: https://www.youtube.com/watch?v=VIDEO_ID
        watch_match = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/)([^&\s]+)', url)
        if watch_match:
            video_id = watch_match.group(1).split('&')[0].split('?')[0]
            return f"https://www.youtube.com/embed/{video_id}"
        
        # Format: https://www.youtube.com/embed/VIDEO_ID
        embed_match = re.search(r'youtube\.com/embed/([^&\s]+)', url)
        if embed_match:
            video_id = embed_match.group(1).split('&')[0].split('?')[0]
            return f"https://www.youtube.com/embed/{video_id}"
        
        return None
    
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
            # Ensure all trailers have embed_url
            for trailer in trailers:
                if not trailer.get("embed_url") and trailer.get("url"):
                    trailer["embed_url"] = self._convert_to_embed_url(trailer.get("url"))
            movie["youtube_trailers"] = trailers
            # Set primary trailer URL if not already set from WatchMode
            if not movie.get("trailer") and trailers:
                movie["trailer"] = trailers[0].get("url")
        
        if isinstance(music_videos, list) and music_videos:
            # Ensure all music videos have embed_url
            for music in music_videos:
                if not music.get("embed_url") and music.get("url"):
                    music["embed_url"] = self._convert_to_embed_url(music.get("url"))
            movie["youtube_music_videos"] = music_videos
        
        return movie
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class AniListClient:
    """Client for interacting with AniList GraphQL API for anime and manga"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        )
    
    async def _execute_query(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query"""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        try:
            response = await self.client.post(self.base_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if "errors" in data:
                print(f"AniList GraphQL errors: {data['errors']}")
                return {}
            
            return data.get("data", {})
        except httpx.HTTPError as e:
            print(f"AniList API error: {str(e)}")
            return {}
    
    def _normalize_anilist_to_omdb_format(self, item: dict, item_type: str = "anime") -> dict:
        """Convert AniList format to OMDB-like format for consistency"""
        title = item.get("title", {})
        title_english = title.get("english") or title.get("romaji") or ""
        
        # Get cover image
        cover_image = item.get("coverImage", {})
        poster = cover_image.get("large") or cover_image.get("medium", "N/A")
        
        # Get year from start date
        start_date = item.get("startDate", {})
        year = str(start_date.get("year", "N/A")) if start_date.get("year") else "N/A"
        
        # Get trailer YouTube ID
        trailer = None
        trailer_data = item.get("trailer", {})
        if trailer_data and trailer_data.get("id"):
            trailer = f"https://www.youtube.com/watch?v={trailer_data['id']}"
        
        # Get studios
        studios = []
        for studio in item.get("studios", {}).get("nodes", []):
            studios.append(studio.get("name", ""))
        
        # Get characters (main characters)
        characters = []
        for char in item.get("characters", {}).get("nodes", [])[:10]:  # Top 10 characters
            characters.append({
                "name": char.get("name", {}).get("full", ""),
                "image": char.get("image", {}).get("large", ""),
                "role": char.get("role", "")
            })
        
        return {
            "Title": title_english,
            "Year": year,
            "imdbID": f"anilist_{item.get('id')}",
            "Type": item_type,
            "Poster": poster,
            "anilist_id": item.get("id"),
            "anilist_rating": item.get("averageScore"),
            "anilist_popularity": item.get("popularity"),
            "anilist_format": item.get("format"),  # TV, MOVIE, OVA, etc.
            "anilist_status": item.get("status"),  # FINISHED, RELEASING, etc.
            "anilist_episodes": item.get("episodes"),  # For anime
            "anilist_chapters": item.get("chapters"),  # For manga
            "anilist_volumes": item.get("volumes"),  # For manga
            "anilist_season": item.get("season"),  # WINTER, SPRING, SUMMER, FALL
            "anilist_seasonYear": item.get("seasonYear"),
            "overview": item.get("description", "").replace("<br>", "\n").replace("<i>", "").replace("</i>", ""),
            "genres": item.get("genres", []),
            "studios": studios,
            "characters": characters,
            "trailer": trailer,
            "trailer_youtube_id": trailer_data.get("id") if trailer_data else None,
            "source": "anilist"
        }
    
    async def search_anime(self, query: str, page: int = 1, per_page: int = 20) -> dict:
        """Search for anime"""
        cache_key = f"anilist_anime_search:{query}:{page}:{per_page}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        graphql_query = """
        query ($search: String, $page: Int, $perPage: Int) {
            Page(page: $page, perPage: $perPage) {
                pageInfo {
                    total
                    currentPage
                    lastPage
                    hasNextPage
                }
                media(search: $search, type: ANIME, sort: POPULARITY_DESC) {
                    id
                    title {
                        romaji
                        english
                    }
                    description
                    coverImage {
                        large
                        medium
                    }
                    startDate {
                        year
                        month
                        day
                    }
                    averageScore
                    popularity
                    format
                    status
                    episodes
                    season
                    seasonYear
                    genres
                    studios {
                        nodes {
                            name
                        }
                    }
                    characters {
                        nodes {
                            name {
                                full
                            }
                            image {
                                large
                            }
                            role
                        }
                    }
                    trailer {
                        id
                        site
                    }
                }
            }
        }
        """
        
        variables = {
            "search": query,
            "page": page,
            "perPage": per_page
        }
        
        data = await self._execute_query(graphql_query, variables)
        
        if not data:
            return {"Response": "False", "Search": [], "totalResults": "0"}
        
        page_data = data.get("Page", {})
        media_list = page_data.get("media", [])
        page_info = page_data.get("pageInfo", {})
        
        # Normalize results
        normalized_results = []
        for item in media_list:
            normalized_results.append(self._normalize_anilist_to_omdb_format(item, "anime"))
        
        result = {
            "Response": "True",
            "Search": normalized_results,
            "totalResults": str(page_info.get("total", len(normalized_results)))
        }
        
        # Cache the response
        cache[cache_key] = (result, datetime.now())
        
        return result
    
    async def search_manga(self, query: str, page: int = 1, per_page: int = 20) -> dict:
        """Search for manga"""
        cache_key = f"anilist_manga_search:{query}:{page}:{per_page}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        graphql_query = """
        query ($search: String, $page: Int, $perPage: Int) {
            Page(page: $page, perPage: $perPage) {
                pageInfo {
                    total
                    currentPage
                    lastPage
                    hasNextPage
                }
                media(search: $search, type: MANGA, sort: POPULARITY_DESC) {
                    id
                    title {
                        romaji
                        english
                    }
                    description
                    coverImage {
                        large
                        medium
                    }
                    startDate {
                        year
                        month
                        day
                    }
                    averageScore
                    popularity
                    format
                    status
                    chapters
                    volumes
                    genres
                    studios {
                        nodes {
                            name
                        }
                    }
                    characters {
                        nodes {
                            name {
                                full
                            }
                            image {
                                large
                            }
                            role
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "search": query,
            "page": page,
            "perPage": per_page
        }
        
        data = await self._execute_query(graphql_query, variables)
        
        if not data:
            return {"Response": "False", "Search": [], "totalResults": "0"}
        
        page_data = data.get("Page", {})
        media_list = page_data.get("media", [])
        page_info = page_data.get("pageInfo", {})
        
        # Normalize results
        normalized_results = []
        for item in media_list:
            normalized_results.append(self._normalize_anilist_to_omdb_format(item, "manga"))
        
        result = {
            "Response": "True",
            "Search": normalized_results,
            "totalResults": str(page_info.get("total", len(normalized_results)))
        }
        
        # Cache the response
        cache[cache_key] = (result, datetime.now())
        
        return result
    
    async def get_popular_anime(self, limit: int = 20, page: int = 1) -> List[dict]:
        """Get popular anime"""
        cache_key = f"anilist_popular_anime:{page}:{limit}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data.get("results", [])[:limit]
        
        graphql_query = """
        query ($page: Int, $perPage: Int) {
            Page(page: $page, perPage: $perPage) {
                media(type: ANIME, sort: POPULARITY_DESC, status: RELEASING) {
                    id
                    title {
                        romaji
                        english
                    }
                    description
                    coverImage {
                        large
                        medium
                    }
                    startDate {
                        year
                    }
                    averageScore
                    popularity
                    format
                    status
                    episodes
                    season
                    seasonYear
                    genres
                    studios {
                        nodes {
                            name
                        }
                    }
                    characters {
                        nodes {
                            name {
                                full
                            }
                            image {
                                large
                            }
                            role
                        }
                    }
                    trailer {
                        id
                        site
                    }
                }
            }
        }
        """
        
        variables = {
            "page": page,
            "perPage": limit
        }
        
        data = await self._execute_query(graphql_query, variables)
        
        if not data:
            return []
        
        media_list = data.get("Page", {}).get("media", [])
        
        # Normalize results
        normalized_results = []
        for item in media_list:
            normalized_results.append(self._normalize_anilist_to_omdb_format(item, "anime"))
        
        # Cache the response
        cache[cache_key] = ({"results": normalized_results}, datetime.now())
        
        return normalized_results[:limit]
    
    async def get_popular_manga(self, limit: int = 20, page: int = 1) -> List[dict]:
        """Get popular manga"""
        cache_key = f"anilist_popular_manga:{page}:{limit}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                # Handle both dict and list cache formats
                if isinstance(cached_data, dict):
                    return cached_data.get("results", [])[:limit]
                elif isinstance(cached_data, list):
                    return cached_data[:limit]
                return []
        
        graphql_query = """
        query ($page: Int, $perPage: Int) {
            Page(page: $page, perPage: $perPage) {
                media(type: MANGA, sort: POPULARITY_DESC) {
                    id
                    title {
                        romaji
                        english
                    }
                    description
                    coverImage {
                        large
                        medium
                    }
                    startDate {
                        year
                    }
                    averageScore
                    popularity
                    format
                    status
                    chapters
                    volumes
                    genres
                    studios {
                        nodes {
                            name
                        }
                    }
                    characters {
                        nodes {
                            name {
                                full
                            }
                            image {
                                large
                            }
                            role
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "page": page,
            "perPage": limit
        }
        
        data = await self._execute_query(graphql_query, variables)
        
        if not data:
            return []
        
        media_list = data.get("Page", {}).get("media", [])
        
        # Normalize results
        normalized_results = []
        for item in media_list:
            normalized_results.append(self._normalize_anilist_to_omdb_format(item, "manga"))
        
        # Cache the response (store as list for consistency)
        cache[cache_key] = (normalized_results, datetime.now())
        
        return normalized_results[:limit]
    
    async def get_anime_by_id(self, anilist_id: int) -> Optional[dict]:
        """Get detailed anime information by AniList ID"""
        cache_key = f"anilist_anime:{anilist_id}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        graphql_query = """
        query ($id: Int) {
            Media(id: $id, type: ANIME) {
                id
                title {
                    romaji
                    english
                }
                description
                coverImage {
                    large
                    medium
                }
                bannerImage
                startDate {
                    year
                    month
                    day
                }
                endDate {
                    year
                    month
                    day
                }
                averageScore
                popularity
                format
                status
                episodes
                duration
                season
                seasonYear
                genres
                studios {
                    nodes {
                        name
                        siteUrl
                    }
                }
                characters {
                    nodes {
                        name {
                            full
                        }
                        image {
                            large
                        }
                        role
                    }
                }
                trailer {
                    id
                    site
                }
                siteUrl
            }
        }
        """
        
        variables = {"id": anilist_id}
        
        data = await self._execute_query(graphql_query, variables)
        
        if not data or not data.get("Media"):
            return None
        
        media = data.get("Media")
        result = self._normalize_anilist_to_omdb_format(media, "anime")
        result["bannerImage"] = media.get("bannerImage")
        result["siteUrl"] = media.get("siteUrl")
        result["duration"] = media.get("duration")
        result["endDate"] = media.get("endDate", {})
        
        # Cache the response
        cache[cache_key] = (result, datetime.now())
        
        return result
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class KitsuClient:
    """Client for interacting with Kitsu API for manga (Free, no API key required)"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "accept": "application/vnd.api+json",
                "content-type": "application/vnd.api+json"
            }
        )
    
    def _normalize_kitsu_to_omdb_format(self, item: dict) -> dict:
        """Convert Kitsu manga format to OMDB-like format for consistency"""
        attributes = item.get("attributes", {})
        
        # Get title
        title = attributes.get("canonicalTitle") or attributes.get("titles", {}).get("en") or attributes.get("titles", {}).get("en_jp", "")
        
        # Get poster
        poster = attributes.get("posterImage", {}).get("large") or attributes.get("posterImage", {}).get("medium", "N/A")
        
        # Get year from start date
        start_date = attributes.get("startDate", "")
        year = str(start_date)[:4] if start_date else "N/A"
        
        # Get description
        description = attributes.get("synopsis", "").replace("<p>", "").replace("</p>", "").replace("<br>", " ").replace("</br>", "") if attributes.get("synopsis") else ""
        
        return {
            "Title": title,
            "Year": year,
            "imdbID": f"kitsu_{item.get('id')}",
            "Type": "manga",
            "Poster": poster,
            "Plot": description,
            "kitsu_id": item.get("id"),
            "kitsu_rating": attributes.get("averageRating"),
            "chapters": attributes.get("chapterCount"),
            "volumes": attributes.get("volumeCount"),
            "status": attributes.get("status"),
            "subtype": attributes.get("subtype"),
            "genres": [g.get("attributes", {}).get("name") for g in attributes.get("genres", {}).get("data", [])] if attributes.get("genres") else [],
            "source": "kitsu"
        }
    
    async def search_manga(self, query: str, limit: int = 20, offset: int = 0) -> List[dict]:
        """Search for manga on Kitsu"""
        cache_key = f"kitsu_manga_search:{query}:{limit}:{offset}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/manga"
            params = {
                "filter[text]": query,
                "page[limit]": limit,
                "page[offset]": offset,
                "sort": "-popularityRank"
            }
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            manga_list = data.get("data", [])
            
            # Normalize results
            normalized_results = []
            for item in manga_list:
                normalized_results.append(self._normalize_kitsu_to_omdb_format(item))
            
            # Cache the response
            cache[cache_key] = (normalized_results, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            print(f"Kitsu API error: {str(e)}")
            return []
    
    async def get_popular_manga(self, limit: int = 20, offset: int = 0) -> List[dict]:
        """Get popular manga from Kitsu"""
        cache_key = f"kitsu_popular_manga:{limit}:{offset}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = f"{self.base_url}/manga"
            params = {
                "page[limit]": limit,
                "page[offset]": offset,
                "sort": "-popularityRank"
            }
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            manga_list = data.get("data", [])
            
            # Normalize results
            normalized_results = []
            for item in manga_list:
                normalized_results.append(self._normalize_kitsu_to_omdb_format(item))
            
            # Cache the response
            cache[cache_key] = (normalized_results, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            print(f"Kitsu API error: {str(e)}")
            return []
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class GutenbergClient:
    """Client for interacting with Gutendex API for Project Gutenberg books (Free, no API key required)"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "accept": "application/json"
            }
        )
    
    def _normalize_gutenberg_to_omdb_format(self, item: dict) -> dict:
        """Convert Gutenberg book format to OMDB-like format for consistency"""
        # Get title
        title = item.get("title", "")
        
        # Get authors
        authors = item.get("authors", [])
        author_names = []
        for author in authors:
            name = author.get("name", "")
            if name:
                author_names.append(name)
        author_str = ", ".join(author_names) if author_names else "Unknown"
        
        # Get cover image (if available)
        formats = item.get("formats", {})
        cover_image = formats.get("image/jpeg") or formats.get("image/jpeg;charset=utf-8") or "N/A"
        
        # Get subjects/genres
        subjects = item.get("subjects", [])
        genres = subjects[:5] if subjects else []  # Limit to 5 subjects
        
        # Get languages
        languages = item.get("languages", [])
        language = languages[0] if languages else "en"
        
        # Get download links and reading URL
        download_links = {}
        reading_url = None
        
        # Get book ID for constructing reading URL
        book_id = item.get("id")
        
        # Get HTML URL for iframe reading
        html_url = formats.get("text/html") or formats.get("text/html;charset=utf-8")
        if html_url:
            download_links["html"] = html_url
            # Use the HTML URL directly if it's from Gutenberg
            if "gutenberg.org" in html_url:
                reading_url = html_url
        
        # Construct Gutenberg reading URL if not set
        # Gutenberg HTML URLs are typically: https://www.gutenberg.org/files/{id}/{id}-h/{id}-h.htm
        if not reading_url and book_id:
            reading_url = f"https://www.gutenberg.org/files/{book_id}/{book_id}-h/{book_id}-h.htm"
        
        if formats.get("application/epub+zip"):
            download_links["epub"] = formats.get("application/epub+zip")
        if formats.get("text/plain"):
            download_links["txt"] = formats.get("text/plain")
        
        # Get year from first published date (if available)
        # Gutenberg books don't always have publication dates, so we'll use copyright year or leave as N/A
        year = "N/A"
        
        return {
            "Title": title,
            "Year": year,
            "imdbID": f"gutenberg_{item.get('id')}",
            "Type": "book",
            "Poster": cover_image,
            "Plot": f"By {author_str}. {', '.join(genres[:3])}" if genres else f"By {author_str}.",
            "gutenberg_id": item.get("id"),
            "authors": author_names,
            "author": author_str,
            "subjects": genres,
            "genres": genres,
            "languages": languages,
            "language": language,
            "download_count": item.get("download_count", 0),
            "download_links": download_links,
            "reading_url": reading_url,  # URL for iframe reading
            "bookshelves": item.get("bookshelves", []),
            "source": "gutenberg"
        }
    
    async def search_books(self, query: str, limit: int = 20, offset: int = 0) -> List[dict]:
        """Search for books on Gutendex"""
        cache_key = f"gutenberg_search:{query}:{limit}:{offset}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            url = self.base_url  # Base URL already includes /books/
            params = {
                "search": query,
                "languages": "en",  # Filter to English books
                "page": (offset // limit) + 1
            }
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            books_list = data.get("results", [])
            
            # Normalize results
            normalized_results = []
            for item in books_list[:limit]:
                normalized_results.append(self._normalize_gutenberg_to_omdb_format(item))
            
            # Cache the response
            cache[cache_key] = (normalized_results, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            print(f"Gutendex API error: {str(e)}")
            return []
    
    async def get_popular_books(self, limit: int = 20, offset: int = 0) -> List[dict]:
        """Get popular books from Gutendex (sorted by download count)"""
        cache_key = f"gutenberg_popular:{limit}:{offset}"
        
        # Check cache
        if cache_key in cache:
            cached_data, cached_time = cache[cache_key]
            if datetime.now() - cached_time < CACHE_DURATION:
                return cached_data
        
        try:
            # Get multiple pages and sort by download count
            all_books = []
            pages_to_fetch = (limit // 32) + 3  # Gutendex returns ~32 books per page, fetch more pages
            
            tasks = []
            for page in range(1, min(pages_to_fetch + 1, 15)):  # Max 15 pages to avoid too many requests
                url = self.base_url  # Base URL already includes /books/
                params = {
                    "languages": "en",
                    "page": page
                }
                tasks.append(self.client.get(url, params=params))
            
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            for response in responses:
                if isinstance(response, Exception):
                    continue
                try:
                    data = response.json()
                    books_list = data.get("results", [])
                    all_books.extend(books_list)
                    if len(all_books) >= limit * 2:  # Get more than needed for better sorting
                        break
                except:
                    continue
            
            # Sort by download count (descending) and take top results
            all_books.sort(key=lambda x: x.get("download_count", 0), reverse=True)
            top_books = all_books[:limit]
            
            # Normalize results
            normalized_results = []
            for item in top_books:
                normalized_results.append(self._normalize_gutenberg_to_omdb_format(item))
            
            # Cache the response
            cache[cache_key] = (normalized_results, datetime.now())
            
            return normalized_results
        except httpx.HTTPError as e:
            print(f"Gutendex API error: {str(e)}")
            return []
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class JustWatchClient:
    """Client for interacting with JustWatch API for direct watch links and streaming availability"""
    
    def __init__(self, base_url: str, country: str = "US"):
        self.base_url = base_url
        self.country = country
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "accept": "application/json",
                "Content-Type": "application/json"
            }
        )
    
    async def get_watch_options(self, title: str, imdb_id: Optional[str] = None, country: Optional[str] = None) -> dict:
        """
        Get watch options for a title
        Returns: {
            "can_watch_directly": bool,
            "direct_watch_url": str or None,
            "platform": str or None,
            "fallback_to_watchmode": bool
        }
        Note: JustWatch doesn't have a public API, so we use WatchMode as primary source
        This can be extended when JustWatch API becomes available
        """
        country = country or self.country
        
        # For now, JustWatch doesn't have a public API
        # We'll enhance WatchMode results to provide better watch options
        return {
            "can_watch_directly": False,
            "direct_watch_url": None,
            "platform": None,
            "fallback_to_watchmode": True,
            "note": "Using WatchMode for streaming availability"
        }
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


def merge_movie_results(omdb_movies: List[dict], tmdb_movies: List[dict], tvmaze_shows: List[dict] = None, anilist_items: List[dict] = None, limit: int = 20) -> List[dict]:
    """Merge and deduplicate movie/anime/manga results from all APIs"""
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
    
    # Add AniList items that aren't duplicates (if provided)
    if anilist_items:
        for item in anilist_items:
            title_key = item.get("Title", "").lower().strip()
            if title_key and title_key not in seen_titles:
                seen_titles.add(title_key)
                merged.append(item)
                if len(merged) >= limit:
                    return merged[:limit]
    
    return merged[:limit]


# Initialize clients (will be set in lifespan)
omdb_client = None
tmdb_client = None
tvmaze_client = None
watchmode_client = None
youtube_client = None
anilist_client = None
kitsu_client = None
gutenberg_client = None
justwatch_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global omdb_client, tmdb_client, tvmaze_client, watchmode_client, youtube_client, anilist_client, kitsu_client, gutenberg_client, justwatch_client
    omdb_client = OMDBClient(OMDB_API_KEY, OMDB_BASE_URL)
    tmdb_client = TMDBClient(TMDB_ACCESS_TOKEN, TMDB_BASE_URL)
    tvmaze_client = TVMazeClient(TVMAZE_BASE_URL)
    watchmode_client = WatchModeClient(WATCHMODE_API_KEY, WATCHMODE_BASE_URL)
    youtube_client = YouTubeClient(YOUTUBE_API_KEY, YOUTUBE_BASE_URL)
    anilist_client = AniListClient(ANILIST_BASE_URL)
    kitsu_client = KitsuClient(KITSU_BASE_URL)
    gutenberg_client = GutenbergClient(GUTENDEX_BASE_URL)
    justwatch_client = JustWatchClient(JUSTWATCH_BASE_URL, JUSTWATCH_COUNTRY)
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
    if anilist_client:
        await anilist_client.close()
    if kitsu_client:
        await kitsu_client.close()
    if gutenberg_client:
        await gutenberg_client.close()
    if justwatch_client:
        await justwatch_client.close()


app = FastAPI(
    title="EVC API",
    description="E-Video Cloud Movie API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware to allow frontend requests
# In production, update allow_origins with your frontend URL
default_origins = "http://localhost:3000,http://localhost:5173,https://moviesgo-lfu1.onrender.com"
cors_origins_str = os.environ.get("CORS_ORIGINS", default_origins)
cors_origins = [origin.strip() for origin in cors_origins_str.split(",") if origin.strip()]

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
        
        # Search all APIs concurrently
        omdb_task = omdb_client.search_movies(query, page, year, omdb_type)
        tmdb_task = tmdb_client.search_movies(query, page, year, type)
        
        # Search AniList for anime
        anilist_result = {"Response": "False", "Search": []}
        if type == "anime":
            anilist_task = anilist_client.search_anime(query, page, per_page=10)
            # Only search TVMaze for series/anime (not movies)
            tvmaze_task = tvmaze_client.search_shows(query, page, year, type)
            omdb_result, tmdb_result, tvmaze_result, anilist_result = await asyncio.gather(
                omdb_task,
                tmdb_task,
                tvmaze_task,
                anilist_task,
                return_exceptions=True
            )
        elif type != "movie":
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
        anilist_items = []
        
        if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
            omdb_movies = omdb_result.get("Search", [])
            # Filter by type if specified
            if type:
                omdb_movies = [m for m in omdb_movies if m.get("Type", "").lower() == type.lower()]
        
        if isinstance(tmdb_result, dict) and tmdb_result.get("Response") == "True":
            tmdb_movies = tmdb_result.get("Search", [])
        
        if isinstance(tvmaze_result, dict) and tvmaze_result.get("Response") == "True":
            tvmaze_shows = tvmaze_result.get("Search", [])
        
        if isinstance(anilist_result, dict) and anilist_result.get("Response") == "True":
            anilist_items = anilist_result.get("Search", [])
        
        # Merge results (limit to 50 per page)
        merged_movies = merge_movie_results(omdb_movies, tmdb_movies, tvmaze_shows, anilist_items, limit=50)
        
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
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, tvmaze_shows, None, limit)
            
        elif type == "anime":
            # Get anime - prioritize AniList for anime
            anilist_task = anilist_client.get_popular_anime(limit)
            omdb_task = omdb_client.search_movies("anime", page=1, type="series")
            tmdb_task = tmdb_client.get_anime_shows(limit)
            tvmaze_task = tvmaze_client.get_popular_shows(limit, content_type="anime")
            
            anilist_items, omdb_result, tmdb_movies, tvmaze_shows = await asyncio.gather(
                anilist_task,
                omdb_task,
                tmdb_task,
                tvmaze_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 4]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            if isinstance(tvmaze_shows, Exception):
                tvmaze_shows = []
            
            if isinstance(anilist_items, Exception):
                anilist_items = []
            
            # Convert anilist_items list to format for merge
            anilist_list = anilist_items if isinstance(anilist_items, list) else []
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, tvmaze_shows, anilist_list, limit)
            
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
            
            # Merge results (no TVMaze or AniList for movies)
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, None, None, limit)
        
        return {"Response": "True", "Search": merged_movies, "totalResults": str(len(merged_movies))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/movies/new-releases")
async def get_new_releases(
    limit: int = Query(6, ge=1, le=20),
    type: Optional[str] = Query(None, description="Type: movie, series, anime")
):
    """Get new releases from OMDB, TMDB, and TVMaze"""
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
            
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, None, None, limit)
            
        elif type == "anime":
            # For anime, get anime shows - prioritize AniList
            anilist_task = anilist_client.get_popular_anime(limit)
            tmdb_task = tmdb_client.get_anime_shows(limit)
            omdb_task = omdb_client.search_movies("anime", page=1, type="series")
            
            anilist_items, tmdb_movies, omdb_result = await asyncio.gather(
                anilist_task,
                tmdb_task,
                omdb_task,
                return_exceptions=True
            )
            
            omdb_movies = []
            if isinstance(omdb_result, dict) and omdb_result.get("Response") == "True":
                omdb_movies = omdb_result.get("Search", [])[:limit // 3]
            
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            if isinstance(anilist_items, Exception):
                anilist_items = []
            
            anilist_list = anilist_items if isinstance(anilist_items, list) else []
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, None, anilist_list, limit)
            
        else:
            # For movies (default), fetch from TMDB's "now playing" endpoint
            tmdb_task = tmdb_client.get_now_playing_movies(limit)
            
            # Also search OMDB for recent movies
            current_year = datetime.now().year
            omdb_tasks = [
                omdb_client.search_movies("the", page=1, year=current_year),
                omdb_client.search_movies("action", page=1, year=current_year - 1)
            ]
            
            try:
                results = await asyncio.gather(
                tmdb_task,
                *omdb_tasks,
                return_exceptions=True
            )
                tmdb_movies = results[0]
                omdb_results = results[1:]
            except Exception as e:
                print(f"Error gathering results: {str(e)}")
                tmdb_movies = []
                omdb_results = []
            
            # Handle exceptions
            if isinstance(tmdb_movies, Exception):
                tmdb_movies = []
            
            # Collect OMDB movies
            omdb_movies = []
            for result in omdb_results:
                if isinstance(result, dict) and result.get("Response") == "True":
                    omdb_movies.extend(result.get("Search", []))
            
            # Merge results
            merged_movies = merge_movie_results(omdb_movies, tmdb_movies, None, None, limit)
            
            # Enrich with full details from OMDB if available (but don't fail if it errors)
            try:
                if merged_movies:
                    merged_movies = await omdb_client.enrich_movie_details(merged_movies)
            except Exception as e:
                print(f"Error enriching movie details: {str(e)}")
                # Continue with unenriched movies
        
        return {"Response": "True", "Search": merged_movies, "totalResults": str(len(merged_movies))}
    except Exception as e:
        import traceback
        error_detail = f"Error in get_new_releases: {str(e)}\n{traceback.format_exc()}"
        print(error_detail)
        # Return empty result instead of raising 500 error
        return {"Response": "True", "Search": [], "totalResults": "0"}


@app.get("/api/movies/{imdb_id}")
async def get_movie(
    imdb_id: str, 
    include_watchmode: bool = Query(True, description="Include WatchMode data (trailers and streaming sources)"),
    include_youtube: bool = Query(True, description="Include YouTube data (trailers and music videos)")
):
    """Get movie details by IMDb ID with optional WatchMode and YouTube enrichment"""
    try:
        result = None
        
        # Handle different ID formats
        if imdb_id.startswith("tmdb_"):
            # TMDB ID
            try:
                tmdb_id = int(imdb_id.replace("tmdb_", ""))
                result = await tmdb_client.get_movie_by_id(tmdb_id, raise_on_error=False)
            except (ValueError, Exception) as e:
                print(f"Error fetching TMDB movie {tmdb_id}: {str(e)}")
                pass
        elif imdb_id.startswith("tvmaze_"):
            # TVMaze ID
            try:
                tvmaze_id = int(imdb_id.replace("tvmaze_", ""))
                result = await tvmaze_client.get_show_by_id(tvmaze_id, raise_on_error=False)
            except (ValueError, Exception) as e:
                print(f"Error fetching TVMaze show {tvmaze_id}: {str(e)}")
                pass
        elif imdb_id.startswith("anilist_"):
            # AniList ID - handled separately in frontend
            raise HTTPException(status_code=404, detail="AniList content handled separately")
        elif imdb_id.startswith("kitsu_"):
            # Kitsu ID - not implemented yet
            raise HTTPException(status_code=404, detail="Kitsu content not yet supported")
        else:
            # Try OMDB first (for regular IMDb IDs)
            result = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=False)
        
            # If not found in OMDB, try TMDB if it looks like a numeric ID
            if (result is None or result.get("Response") == "False") and imdb_id.isdigit():
                try:
                    tmdb_id = int(imdb_id)
                    result = await tmdb_client.get_movie_by_id(tmdb_id, raise_on_error=False)
                except (ValueError, Exception):
                    pass
        
        if result is None or result.get("Response") == "False":
            raise HTTPException(status_code=404, detail=f"Movie not found for ID: {imdb_id}")
        
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
        print(f"Error in get_movie: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return 404 instead of 500 if it's a not found error
        if "not found" in str(e).lower() or "404" in str(e):
            raise HTTPException(status_code=404, detail="Movie not found")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/movies/{imdb_id}/streaming")
async def get_movie_streaming(imdb_id: str):
    """Get streaming platform sources for a movie by IMDb ID"""
    try:
        # First get the movie to get the title - don't raise error
        movie = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=False)
        if movie is None or movie.get("Response") == "False":
            # Return empty instead of error
            return {"streaming_sources": [], "trailer": None}
        
        # Search for the movie in WatchMode
        title = movie.get("Title", "")
        if not title:
            return {"streaming_sources": [], "trailer": None}
        
        try:
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
        except Exception as e:
            print(f"WatchMode error in streaming endpoint: {str(e)}")
            return {"streaming_sources": [], "trailer": None}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_movie_streaming: {str(e)}")
        # Return empty instead of error
        return {"streaming_sources": [], "trailer": None}


@app.get("/api/movies/{imdb_id}/watch")
async def get_movie_watch_options(imdb_id: str, country: str = Query("US", description="Country code for streaming availability")):
    """
    Get comprehensive watch options for a movie
    Checks JustWatch first (if available), then falls back to WatchMode
    Returns: can_watch_directly, direct_watch_url, streaming_sources, platforms
    """
    try:
        # Get movie details first - don't raise error, return empty if not found
        movie = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=False)
        if movie is None or movie.get("Response") == "False":
            # Return empty watch options instead of error
            return {
                "can_watch_directly": False,
                "direct_watch_url": None,
                "primary_platform": None,
                "streaming_sources": [],
                "trailer": None,
                "available_platforms": [],
                "platform_count": 0,
                "source": "none"
            }
        
        title = movie.get("Title", "")
        
        # Try to get TMDB ID from multiple sources
        tmdb_id_from_movie = movie.get("tmdb_id")
        if not tmdb_id_from_movie:
            # Check if imdb_id is actually a TMDB ID
            if imdb_id.startswith("tmdb_"):
                try:
                    tmdb_id_from_movie = int(imdb_id.replace("tmdb_", ""))
                except:
                    pass
            # Also check if movie was enriched with TMDB data
            if not tmdb_id_from_movie and movie.get("source") == "tmdb":
                # Extract from imdbID if it's in format tmdb_123
                imdb_id_str = str(movie.get("imdbID", ""))
                if imdb_id_str.startswith("tmdb_"):
                    try:
                        tmdb_id_from_movie = int(imdb_id_str.replace("tmdb_", ""))
                    except:
                        pass
        
        # Check JustWatch for direct watch (currently returns fallback)
        justwatch_options = await justwatch_client.get_watch_options(title, imdb_id, country)
        
        # Get WatchMode streaming sources
        watchmode_sources = []
        trailer = None
        
        # Get TMDB watch providers as fallback/enhancement
        tmdb_providers = None
        if tmdb_id_from_movie:
            try:
                is_tv = movie.get("Type", "").lower() in ["series", "episode"]
                tmdb_providers = await tmdb_client.get_watch_providers(tmdb_id_from_movie, is_tv=is_tv, country=country)
                if tmdb_providers:
                    print(f"TMDB watch providers found for tmdb_id={tmdb_id_from_movie}")
            except Exception as e:
                print(f"TMDB watch providers error: {str(e)}")
        elif imdb_id and not imdb_id.startswith("tmdb_") and not imdb_id.startswith("anilist_") and not imdb_id.startswith("tvmaze_"):
            # Try to find TMDB ID by IMDb ID using TMDB's find API
            try:
                # TMDB find by external ID endpoint
                find_url = f"{tmdb_client.base_url}/find/{imdb_id}"
                find_params = {"external_source": "imdb_id"}
                find_response = await tmdb_client.client.get(find_url, params=find_params)
                if find_response.status_code == 200:
                    find_data = find_response.json()
                    # Check movie results first
                    movie_results = find_data.get("movie_results", [])
                    if movie_results:
                        tmdb_id_from_movie = movie_results[0].get("id")
                        tmdb_providers = await tmdb_client.get_watch_providers(tmdb_id_from_movie, is_tv=False, country=country)
                        if tmdb_providers:
                            print(f"TMDB watch providers found via IMDb ID lookup: tmdb_id={tmdb_id_from_movie}")
                    else:
                        # Try TV results
                        tv_results = find_data.get("tv_results", [])
                        if tv_results:
                            tmdb_id_from_movie = tv_results[0].get("id")
                            tmdb_providers = await tmdb_client.get_watch_providers(tmdb_id_from_movie, is_tv=True, country=country)
                            if tmdb_providers:
                                print(f"TMDB watch providers found via IMDb ID lookup (TV): tmdb_id={tmdb_id_from_movie}")
            except Exception as e:
                print(f"TMDB find by IMDb ID error: {str(e)}")
        
        try:
            # First try searching by title
            watchmode_results = await watchmode_client.search_titles(title)
            print(f"WatchMode search for '{title}': Found {len(watchmode_results) if watchmode_results else 0} results")
            
            # If no results, try with year if available
            if not watchmode_results and movie.get("Year"):
                year_query = f"{title} {movie.get('Year')}"
                watchmode_results = await watchmode_client.search_titles(year_query)
                print(f"WatchMode search with year '{year_query}': Found {len(watchmode_results) if watchmode_results else 0} results")
            
            if watchmode_results:
                # Try to find best match by IMDb ID if available
                title_id = None
                for result in watchmode_results:
                    result_imdb = result.get("imdb_id", "")
                    # Check if IMDb IDs match (handle both with and without 'tt' prefix)
                    if imdb_id and result_imdb:
                        if imdb_id.replace("tt", "") == result_imdb.replace("tt", ""):
                            title_id = result.get("id")
                            print(f"Found exact IMDb match: {imdb_id} -> WatchMode title_id: {title_id}")
                            break
                
                # If no IMDb match, use first result
                if not title_id:
                    title_id = watchmode_results[0].get("id")
                    print(f"Using first search result: title_id={title_id}, imdb_id={watchmode_results[0].get('imdb_id')}")
                
                if title_id:
                    details_task = watchmode_client.get_title_details(title_id)
                    sources_task = watchmode_client.get_title_sources(title_id)
                    
                    details, sources = await asyncio.gather(
                        details_task,
                        sources_task,
                        return_exceptions=True
                    )
                    
                    if isinstance(details, Exception):
                        print(f"WatchMode details error: {details}")
                    elif isinstance(details, dict):
                        trailer = details.get("trailer")
                        print(f"WatchMode trailer: {trailer is not None}")
                    
                    if isinstance(sources, Exception):
                        print(f"WatchMode sources error: {sources}")
                    elif isinstance(sources, list):
                        watchmode_sources = sources
                        print(f"WatchMode sources found: {len(watchmode_sources)} platforms")
                    else:
                        print(f"WatchMode sources unexpected type: {type(sources)}")
            else:
                print(f"No WatchMode results found for movie: {title} (imdb_id: {imdb_id})")
        except Exception as e:
            print(f"WatchMode error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # Helper function to generate platform URLs
        def get_platform_url(platform_name: str, title: str) -> str:
            """Generate a URL for a streaming platform"""
            platform_lower = platform_name.lower().replace(" ", "").replace("+", "plus")
            
            # Map common platforms to their URLs
            platform_urls = {
                "netflix": f"https://www.netflix.com/search?q={title.replace(' ', '%20')}",
                "amazonprimevideo": "https://www.primevideo.com",
                "amazonprime": "https://www.primevideo.com",
                "disneyplus": "https://www.disneyplus.com",
                "hulu": "https://www.hulu.com",
                "hbo": "https://www.hbo.com",
                "hbomax": "https://www.hbomax.com",
                "max": "https://www.hbomax.com",
                "paramountplus": "https://www.paramountplus.com",
                "peacock": "https://www.peacocktv.com",
                "appletv": "https://tv.apple.com",
                "appletv+": "https://tv.apple.com",
                "youtube": f"https://www.youtube.com/results?search_query={title.replace(' ', '+')}+full+movie",
                "youtubepremium": f"https://www.youtube.com/results?search_query={title.replace(' ', '+')}+full+movie",
                "crunchyroll": "https://www.crunchyroll.com",
                "fubotv": "https://www.fubo.tv",
                "showtime": "https://www.showtime.com",
                "starz": "https://www.starz.com",
                "fxnow": "https://www.fxnetworks.com",
                "tubi": "https://tubitv.com",
                "plutotv": "https://pluto.tv",
                "vudu": "https://www.vudu.com",
                "googleplay": f"https://play.google.com/store/search?q={title.replace(' ', '%20')}&c=movies",
                "playstore": f"https://play.google.com/store/search?q={title.replace(' ', '%20')}&c=movies",
                "itunes": f"https://tv.apple.com/search?term={title.replace(' ', '%20')}",
                "appstore": f"https://apps.apple.com/us/search?term={title.replace(' ', '%20')}",
            }
            
            # Try exact match first
            if platform_lower in platform_urls:
                return platform_urls[platform_lower]
            
            # Try partial matches
            for key, url in platform_urls.items():
                if key in platform_lower or platform_lower in key:
                    return url
            
            # Default: JustWatch search
            return f"https://www.justwatch.com/us/search?q={title.replace(' ', '%20')}"
        
        # Format streaming sources from WatchMode
        formatted_sources = []
        platform_names = set()
        
        for source in watchmode_sources:
            source_type = source.get("type", "").lower()  # free, subscription, rental, buy
            platform_name = source.get("name", "")
            
            if platform_name:
                platform_names.add(platform_name)
            
            # Determine if we can watch directly (free or subscription)
            can_watch = source_type in ["free", "subscription"]
            
            # Check if WatchMode source is YouTube
            web_url = source.get("web_url") or source.get("url")
            is_youtube_watchmode = web_url and ("youtube.com" in str(web_url) or "youtu.be" in str(web_url))
            
            formatted_source = {
                "platform": platform_name,
                "name": platform_name,
                "type": source_type,
                "can_watch_directly": can_watch,
                "web_url": web_url,
                "direct_watch_url": web_url,
                "url": web_url,
                "ios_url": source.get("ios_url"),
                "android_url": source.get("android_url"),
                "playstore_url": source.get("android_url"),  # Play Store link
                "format": source.get("format"),  # sd, hd, 4k
                "price": source.get("price"),
                "price_display": f"${source.get('price', 0):.2f}" if source.get("price") else None,
                "source": "watchmode"
            }
            
            # If WatchMode source is YouTube, add YouTube metadata
            if is_youtube_watchmode:
                import re
                video_id_match = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/|embed/)([^&\s]+)', str(web_url))
                if video_id_match:
                    video_id = video_id_match.group(1).split('&')[0].split('?')[0]
                    formatted_source.update({
                        "source": "youtube",
                        "youtube_video_id": video_id,
                        "youtube_embed_url": f"https://www.youtube.com/embed/{video_id}",
                        "platform": "YouTube",
                        "name": "YouTube"
                    })
                    platform_names.add("YouTube")
            
            formatted_sources.append(formatted_source)
        
        # Helper function to generate platform URLs
        def get_platform_url(platform_name: str, title: str) -> str:
            """Generate a URL for a streaming platform"""
            platform_lower = platform_name.lower().replace(" ", "").replace("+", "plus")
            
            # Map common platforms to their URLs
            platform_urls = {
                "netflix": f"https://www.netflix.com/search?q={title.replace(' ', '%20')}",
                "amazonprimevideo": "https://www.primevideo.com",
                "amazonprime": "https://www.primevideo.com",
                "disneyplus": "https://www.disneyplus.com",
                "hulu": "https://www.hulu.com",
                "hbo": "https://www.hbo.com",
                "hbomax": "https://www.hbomax.com",
                "max": "https://www.hbomax.com",
                "paramountplus": "https://www.paramountplus.com",
                "peacock": "https://www.peacocktv.com",
                "appletv": "https://tv.apple.com",
                "appletv+": "https://tv.apple.com",
                "youtube": f"https://www.youtube.com/results?search_query={title.replace(' ', '+')}+full+movie",
                "youtubepremium": f"https://www.youtube.com/results?search_query={title.replace(' ', '+')}+full+movie",
                "crunchyroll": "https://www.crunchyroll.com",
                "fubotv": "https://www.fubo.tv",
                "showtime": "https://www.showtime.com",
                "starz": "https://www.starz.com",
                "fxnow": "https://www.fxnetworks.com",
                "tubi": "https://tubitv.com",
                "plutotv": "https://pluto.tv",
                "vudu": "https://www.vudu.com",
                "googleplay": f"https://play.google.com/store/search?q={title.replace(' ', '%20')}&c=movies",
                "playstore": f"https://play.google.com/store/search?q={title.replace(' ', '%20')}&c=movies",
                "itunes": f"https://tv.apple.com/search?term={title.replace(' ', '%20')}",
                "appstore": f"https://apps.apple.com/us/search?term={title.replace(' ', '%20')}",
            }
            
            # Try exact match first
            if platform_lower in platform_urls:
                return platform_urls[platform_lower]
            
            # Try partial matches
            for key, url in platform_urls.items():
                if key in platform_lower or platform_lower in key:
                    return url
            
            # Default: JustWatch search
            return f"https://www.justwatch.com/us/search?q={title.replace(' ', '%20')}"
        
        # Add TMDB watch providers if available (merge with WatchMode results)
        # Always try to get TMDB providers, even if WatchMode returned results
        if tmdb_providers and tmdb_providers.get("results"):
            # TMDB structure: {results: {US: {flatrate: [...], rent: [...], buy: [...]}}}
            country_data = tmdb_providers.get("results", {}).get(country, {})
            print(f"TMDB country data for {country}: {list(country_data.keys())}")
            
            # Flatrate = subscription streaming
            flatrate = country_data.get("flatrate", [])
            print(f"📺 TMDB flatrate providers: {len(flatrate)} - {[p.get('provider_name') for p in flatrate[:5]]}")
            for provider in flatrate:
                provider_name = provider.get("provider_name", "")
                if provider_name and provider_name not in platform_names:
                    platform_names.add(provider_name)
                    platform_url = get_platform_url(provider_name, title)
                    formatted_sources.append({
                        "platform": provider_name,
                        "name": provider_name,
                        "type": "subscription",
                        "can_watch_directly": True,
                        "web_url": platform_url,
                        "direct_watch_url": platform_url,
                        "url": platform_url,
                        "ios_url": platform_url,
                        "android_url": platform_url,
                        "playstore_url": platform_url,
                        "format": None,
                        "price": None,
                        "price_display": None,
                        "source": "tmdb",
                        "tmdb_provider_id": provider.get("provider_id"),
                        "logo_path": provider.get("logo_path")
                    })
            
            # Rent = rental streaming
            rent = country_data.get("rent", [])
            for provider in rent:
                provider_name = provider.get("provider_name", "")
                if provider_name and provider_name not in platform_names:
                    platform_names.add(provider_name)
                    platform_url = get_platform_url(provider_name, title)
                    formatted_sources.append({
                        "platform": provider_name,
                        "name": provider_name,
                        "type": "rental",
                        "can_watch_directly": False,
                        "web_url": platform_url,
                        "direct_watch_url": platform_url,
                        "url": platform_url,
                        "ios_url": platform_url,
                        "android_url": platform_url,
                        "playstore_url": platform_url,
                        "format": None,
                        "price": None,
                        "price_display": None,
                        "source": "tmdb",
                        "tmdb_provider_id": provider.get("provider_id"),
                        "logo_path": provider.get("logo_path")
                    })
            
            # Buy = purchase streaming
            buy = country_data.get("buy", [])
            for provider in buy:
                provider_name = provider.get("provider_name", "")
                if provider_name and provider_name not in platform_names:
                    platform_names.add(provider_name)
                    platform_url = get_platform_url(provider_name, title)
                    formatted_sources.append({
                        "platform": provider_name,
                        "name": provider_name,
                        "type": "buy",
                        "can_watch_directly": False,
                        "web_url": platform_url,
                        "direct_watch_url": platform_url,
                        "url": platform_url,
                        "ios_url": platform_url,
                        "android_url": platform_url,
                        "playstore_url": platform_url,
                        "format": None,
                        "price": None,
                        "price_display": None,
                        "source": "tmdb",
                        "tmdb_provider_id": provider.get("provider_id"),
                        "logo_path": provider.get("logo_path")
                    })
            
            # Free = free streaming
            free = country_data.get("free", [])
            for provider in free:
                provider_name = provider.get("provider_name", "")
                if provider_name and provider_name not in platform_names:
                    platform_names.add(provider_name)
                    platform_url = get_platform_url(provider_name, title)
                    formatted_sources.append({
                        "platform": provider_name,
                        "name": provider_name,
                        "type": "free",
                        "can_watch_directly": True,
                        "web_url": platform_url,
                        "direct_watch_url": platform_url,
                        "url": platform_url,
                        "ios_url": platform_url,
                        "android_url": platform_url,
                        "playstore_url": platform_url,
                        "format": None,
                        "price": None,
                        "price_display": None,
                        "source": "tmdb",
                        "tmdb_provider_id": provider.get("provider_id"),
                        "logo_path": provider.get("logo_path")
                    })
            
            if formatted_sources:
                print(f"Added {len(formatted_sources) - len(watchmode_sources)} TMDB providers")
        
        # Add YouTube as streaming source if movie is available on YouTube
        # Search for full movie on YouTube (not just trailers)
        # Only if we don't already have a YouTube source from WatchMode
        has_youtube_source = any("YouTube" in str(s.get("platform", "")) or s.get("source") == "youtube" for s in formatted_sources)
        
        if not has_youtube_source:
            try:
                year = movie.get("Year", "")
                youtube_query = f"{title} full movie {year}" if year else f"{title} full movie"
                youtube_videos = await youtube_client.search_videos(
                    youtube_query, 
                    max_results=5,  # Search more videos for better matches
                    video_type="video"
                )
                
                # Filter for videos that look like full movies (longer duration, keywords in title)
                if youtube_videos:
                    for video in youtube_videos:
                        video_title = video.get("title", "").lower()
                        video_url = video.get("url")
                        video_id = video.get("video_id")
                        
                        if not video_url or not video_id:
                            continue
                        
                        # Check if video title suggests it's a full movie (not trailer, clip, etc.)
                        is_full_movie = (
                            "full movie" in video_title or
                            "complete movie" in video_title or
                            "full film" in video_title or
                            ("movie" in video_title and "trailer" not in video_title and 
                             "clip" not in video_title and "scene" not in video_title and
                             "teaser" not in video_title and "preview" not in video_title)
                        )
                        
                        if is_full_movie:
                            formatted_sources.append({
                                "platform": "YouTube",
                                "name": "YouTube",
                                "type": "free",
                                "can_watch_directly": True,
                                "web_url": video_url,
                                "direct_watch_url": video_url,
                                "url": video_url,
                                "android_url": video_url,
                                "ios_url": video_url,
                                "playstore_url": video_url,
                                "format": None,
                                "price": None,
                                "price_display": None,
                                "source": "youtube",
                                "youtube_video_id": video_id,
                                "youtube_embed_url": video.get("embed_url") or f"https://www.youtube.com/embed/{video_id}",
                                "youtube_title": video.get("title")
                            })
                            platform_names.add("YouTube")
                            print(f"✅ Added YouTube streaming source: {video.get('title')} (ID: {video_id})")
                            break  # Only add the first valid full movie
            except Exception as e:
                print(f"⚠️ YouTube search error for streaming: {str(e)}")
        else:
            print(f"✅ YouTube source already found from WatchMode")
        
        # Check if any source allows direct watching
        can_watch_directly = any(s.get("can_watch_directly") for s in formatted_sources)
        primary_watch_url = None
        primary_platform = None
        
        if can_watch_directly:
            # Find the first free or subscription source
            for source in formatted_sources:
                if source.get("can_watch_directly"):
                    primary_watch_url = source.get("web_url") or source.get("android_url") or source.get("ios_url")
                    primary_platform = source.get("platform")
                    break
        
        # Determine source type
        source_type = "none"
        if formatted_sources:
            has_watchmode = any(s.get("source") == "watchmode" for s in formatted_sources)
            has_tmdb = any(s.get("source") == "tmdb" for s in formatted_sources)
            if has_watchmode and has_tmdb:
                source_type = "watchmode+tmdb"
            elif has_watchmode:
                source_type = "watchmode"
            elif has_tmdb:
                source_type = "tmdb"
        
        # Log final result for debugging
        print(f"📊 Final watch options: {len(formatted_sources)} sources, platforms: {sorted(list(platform_names))}")
        
        return {
            "can_watch_directly": can_watch_directly,
            "direct_watch_url": primary_watch_url,
            "primary_platform": primary_platform,
            "streaming_sources": formatted_sources,
            "trailer": trailer,
            "available_platforms": sorted(list(platform_names)),
            "platform_count": len(platform_names),
            "source": source_type
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_movie_watch_options: {str(e)}")
        # Return empty watch options instead of error
        return {
            "can_watch_directly": False,
            "direct_watch_url": None,
            "primary_platform": None,
            "streaming_sources": [],
            "trailer": None,
            "available_platforms": [],
            "platform_count": 0,
            "source": "error"
        }


@app.get("/api/movies/{imdb_id}/youtube")
async def get_movie_youtube(imdb_id: str, include_music: bool = Query(True, description="Include music videos")):
    """Get YouTube videos (trailers and music) for a movie by IMDb ID"""
    try:
        # First get the movie to get the title - don't raise error
        movie = await omdb_client.get_movie_by_id(imdb_id, raise_on_error=False)
        if movie is None or movie.get("Response") == "False":
            # Return empty instead of error
            return {"youtube_trailers": [], "youtube_music_videos": []}
        
        title = movie.get("Title", "")
        year = movie.get("Year", "")
        
        if not title:
            return {"youtube_trailers": [], "youtube_music_videos": []}
        
        # Search for trailers and optionally music videos
        try:
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
        except Exception as e:
            print(f"YouTube search error: {str(e)}")
            return {"youtube_trailers": [], "youtube_music_videos": []}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_movie_youtube: {str(e)}")
        # Return empty instead of error
        return {"youtube_trailers": [], "youtube_music_videos": []}


@app.get("/api/manga/search")
async def search_manga(
    query: str = Query(..., description="Search query for manga"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(200, ge=1, le=300, description="Results per page")
):
    """Search for manga using AniList, Kitsu, and Gutenberg APIs"""
    try:
        # Check if clients are initialized
        if not anilist_client or not kitsu_client or not gutenberg_client:
            print("Warning: Manga clients not initialized")
            return {
                "Response": "True",
                "Search": [],
                "totalResults": "0"
            }
        
        # Fetch from all three APIs concurrently
        offset = (page - 1) * limit
        # Increase per_source to get more results, especially for Gutenberg
        per_source = (limit // 3) + 20  # Divide limit among 3 sources, add buffer
        
        anilist_task = anilist_client.search_manga(query, page, per_page=per_source)
        kitsu_task = kitsu_client.search_manga(query, per_source, offset)
        gutenberg_task = gutenberg_client.search_books(query, per_source, offset)
        
        anilist_result, kitsu_manga, gutenberg_books = await asyncio.gather(
            anilist_task,
            kitsu_task,
            gutenberg_task,
            return_exceptions=True
        )
        
        # Extract manga from AniList result
        anilist_manga = []
        if isinstance(anilist_result, Exception):
            print(f"AniList search error: {anilist_result}")
            anilist_manga = []
        elif isinstance(anilist_result, dict) and anilist_result.get("Response") == "True":
            anilist_manga = anilist_result.get("Search", [])
        elif isinstance(anilist_result, dict):
            # Sometimes AniList might return data in a different format
            if "Search" in anilist_result:
                anilist_manga = anilist_result.get("Search", [])
        
        if isinstance(kitsu_manga, Exception):
            print(f"Kitsu search error: {kitsu_manga}")
            kitsu_manga = []
        elif not isinstance(kitsu_manga, list):
            print(f"Kitsu returned unexpected type: {type(kitsu_manga)}")
            kitsu_manga = []
        
        if isinstance(gutenberg_books, Exception):
            print(f"Gutenberg search error: {gutenberg_books}")
            gutenberg_books = []
        elif not isinstance(gutenberg_books, list):
            print(f"Gutenberg returned unexpected type: {type(gutenberg_books)}")
            gutenberg_books = []
        
        # Merge results
        all_manga = anilist_manga + kitsu_manga + gutenberg_books
        
        print(f"Manga search: Query='{query}', AniList={len(anilist_manga)}, Kitsu={len(kitsu_manga)}, Gutenberg={len(gutenberg_books)}, Total={len(all_manga)}")
        
        return {
            "Response": "True",
            "Search": all_manga[:limit],
            "totalResults": str(len(all_manga))
        }
    except Exception as e:
        print(f"Error in search_manga: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty results instead of error to prevent frontend issues
        return {
            "Response": "True",
            "Search": [],
            "totalResults": "0"
        }


@app.get("/api/manga/popular")
async def get_popular_manga(
    limit: int = Query(200, ge=1, le=300, description="Number of results")
):
    """Get popular manga from AniList, Kitsu, and Gutenberg"""
    try:
        # Check if clients are initialized
        if not anilist_client or not kitsu_client or not gutenberg_client:
            print("Warning: Manga clients not initialized")
            return {
                "Response": "True",
                "Search": [],
                "totalResults": "0"
            }
        
        # Fetch from all three APIs concurrently
        # Increase per_source to get more results, especially for Gutenberg
        per_source = (limit // 3) + 20  # Divide limit among 3 sources, add buffer
        
        anilist_task = anilist_client.get_popular_manga(per_source)
        kitsu_task = kitsu_client.get_popular_manga(per_source)
        gutenberg_task = gutenberg_client.get_popular_books(per_source)
        
        anilist_result, kitsu_manga, gutenberg_books = await asyncio.gather(
            anilist_task,
            kitsu_task,
            gutenberg_task,
            return_exceptions=True
        )
        
        # Extract manga from AniList result (it returns a list directly)
        anilist_manga = []
        if isinstance(anilist_result, list):
            anilist_manga = anilist_result
        elif isinstance(anilist_result, Exception):
            print(f"AniList error: {anilist_result}")
            anilist_manga = []
        elif isinstance(anilist_result, dict):
            # Sometimes AniList might return data in a dict format
            if "Search" in anilist_result:
                anilist_manga = anilist_result.get("Search", [])
            elif "results" in anilist_result:
                anilist_manga = anilist_result.get("results", [])
        else:
            # Unexpected type
            print(f"AniList returned unexpected type: {type(anilist_result)}")
            anilist_manga = []
        
        if isinstance(kitsu_manga, Exception):
            print(f"Kitsu error: {kitsu_manga}")
            kitsu_manga = []
        elif not isinstance(kitsu_manga, list):
            print(f"Kitsu returned unexpected type: {type(kitsu_manga)}")
            kitsu_manga = []
        
        if isinstance(gutenberg_books, Exception):
            print(f"Gutenberg error: {gutenberg_books}")
            gutenberg_books = []
        elif not isinstance(gutenberg_books, list):
            print(f"Gutenberg returned unexpected type: {type(gutenberg_books)}")
            gutenberg_books = []
        
        # Merge results
        all_manga = anilist_manga + kitsu_manga + gutenberg_books
        
        print(f"Manga popular: AniList={len(anilist_manga)}, Kitsu={len(kitsu_manga)}, Gutenberg={len(gutenberg_books)}, Total={len(all_manga)}")
        
        # Always return a valid response structure, even if empty
        result_count = len(all_manga)
        print(f"Returning {result_count} manga items (limit={limit})")
        return {
            "Response": "True",
            "Search": all_manga[:limit] if all_manga else [],
            "totalResults": str(result_count)
        }
    except Exception as e:
        print(f"Error in get_popular_manga: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty results instead of error to prevent frontend issues
        return {
            "Response": "True",
            "Search": [],
            "totalResults": "0",
            "Error": str(e)
        }


@app.get("/api/anime/{anilist_id}")
async def get_anime_by_id(anilist_id: int):
    """Get detailed anime information by AniList ID"""
    try:
        result = await anilist_client.get_anime_by_id(anilist_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Anime not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/books/{gutenberg_id}")
async def get_book_by_id(gutenberg_id: int):
    """Get book details by Gutenberg ID with reading URL"""
    try:
        # Fetch book details from Gutendex
        # GUTENDEX_BASE_URL is "https://gutendex.com/books/", so we need to append the ID
        url = f"{GUTENDEX_BASE_URL.rstrip('/')}/{gutenberg_id}"
        print(f"Fetching book from: {url}")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        
        if not data:
            print(f"Book {gutenberg_id} not found - empty response")
            raise HTTPException(status_code=404, detail="Book not found")
        
        # Normalize to our format - use global client if available, otherwise create temp instance
        if gutenberg_client:
            book = gutenberg_client._normalize_gutenberg_to_omdb_format(data)
        else:
            # Create temporary client for normalization if global client not available
            temp_client = GutenbergClient(GUTENDEX_BASE_URL)
            book = temp_client._normalize_gutenberg_to_omdb_format(data)
        
        # Ensure gutenberg_id is set
        if not book.get("gutenberg_id"):
            book["gutenberg_id"] = gutenberg_id or data.get("id")
        
        # Ensure reading URL is set - try multiple formats
        if not book.get("reading_url"):
            book_id = book.get("gutenberg_id") or gutenberg_id
            if book_id:
                # Try standard format first
                book["reading_url"] = f"https://www.gutenberg.org/files/{book_id}/{book_id}-h/{book_id}-h.htm"
        
        # Also ensure download_links has html if we have a reading_url
        if book.get("reading_url") and not book.get("download_links", {}).get("html"):
            book.setdefault("download_links", {})["html"] = book["reading_url"]
        
        # Ensure source and Type are set
        book["source"] = "gutenberg"
        book["Type"] = "book"
        
        print(f"Successfully loaded book {gutenberg_id}: {book.get('Title')}")
        return book
    except httpx.HTTPStatusError as e:
        print(f"HTTP error fetching book {gutenberg_id}: {e.response.status_code} - {e}")
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Book not found")
        raise HTTPException(status_code=500, detail=f"Error fetching book: {str(e)}")
    except Exception as e:
        print(f"Error in get_book_by_id for {gutenberg_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching book: {str(e)}")


@app.get("/api/actors/popular")
async def get_popular_actors(page: int = Query(1, ge=1)):
    """Get popular actors from TMDB"""
    try:
        actors = await tmdb_client.get_popular_people(page)
        return {
            "Response": "True",
            "results": actors,
            "page": page
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/actors/{person_id}")
async def get_actor_details(person_id: int):
    """Get actor details with biography and filmography from TMDB"""
    try:
        actor = await tmdb_client.get_person_details(person_id)
        if actor is None:
            raise HTTPException(status_code=404, detail="Actor not found")
        return actor
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/actors/search")
async def search_actors(query: str = Query(..., description="Search query for actors"), page: int = Query(1, ge=1)):
    """Search for actors by name"""
    try:
        results = await tmdb_client.search_people(query, page)
        return {
            "Response": "True",
            "results": results,
            "page": page
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/music/search")
async def search_music_videos(
    query: str = Query(..., description="Search query for music videos"),
    max_results: int = Query(20, ge=1, le=50, description="Maximum number of results")
):
    """Search for music videos on YouTube"""
    try:
        videos = await youtube_client.search_music_videos(query, max_results)
        return {
            "Response": "True",
            "videos": videos,
            "totalResults": len(videos)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/music/trending")
async def get_trending_music_videos(
    max_results: int = Query(50, ge=1, le=50, description="Maximum number of results")
):
    """Get trending music videos on YouTube"""
    try:
        # Use popular music search queries and sort by view count
        trending_queries = [
            "music",
            "popular songs",
            "top hits",
            "latest music"
        ]
        
        # Fetch from multiple queries and combine
        all_videos = []
        videos_per_query = (max_results // len(trending_queries)) + 5
        
        # Search with viewCount order for trending
        tasks = []
        for query in trending_queries:
            cache_key = f"youtube_search:{query}:{videos_per_query}:music:video"
            
            # Check cache
            if cache_key in cache:
                cached_data, cached_time = cache[cache_key]
                if datetime.now() - cached_time < CACHE_DURATION:
                    tasks.append(cached_data)
                    continue
            
            # Create task to search with viewCount order
            url = f"{youtube_client.base_url}/search"
            params = {
                "key": youtube_client.api_key,
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": videos_per_query,
                "order": "viewCount",  # Sort by view count for trending
                "videoCategoryId": "10"  # Music category
            }
            
            tasks.append(youtube_client.client.get(url, params=params))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        seen_video_ids = set()
        for result in results:
            if isinstance(result, list):
                # Already processed cached data
                for video in result:
                    video_id = video.get("video_id")
                    if video_id and video_id not in seen_video_ids:
                        seen_video_ids.add(video_id)
                        all_videos.append(video)
            elif hasattr(result, 'json'):
                # HTTP response - process it
                try:
                    data = result.json()
                    for item in data.get("items", []):
                        video_id = item.get("id", {}).get("videoId")
                        if video_id and video_id not in seen_video_ids:
                            snippet = item.get("snippet", {})
                            seen_video_ids.add(video_id)
                            all_videos.append({
                                "video_id": video_id,
                                "title": snippet.get("title"),
                                "description": snippet.get("description"),
                                "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url") or snippet.get("thumbnails", {}).get("medium", {}).get("url"),
                                "channel_title": snippet.get("channelTitle"),
                                "published_at": snippet.get("publishedAt"),
                                "url": f"https://www.youtube.com/watch?v={video_id}",
                                "embed_url": f"https://www.youtube.com/embed/{video_id}"
                            })
                except:
                    pass
            
            if len(all_videos) >= max_results:
                break
        
        return {
            "Response": "True",
            "videos": all_videos[:max_results],
            "totalResults": len(all_videos)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

