"""
Geocoding and geographic data processing
"""
import pandas as pd
import logging
import requests
from typing import Iterator
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class GeocodingProcessor:
    """Process geocoding data using Census geocoder"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
    
    def add_geocoding_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Add geocoding data (lat/lng/CBSA) using Census geocoder"""
        logger.info("Adding geocoding data using Census geocoder...")
        
        def geocode_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'postal_code' not in chunk.columns:
                logger.warning("No postal_code column found for geocoding")
                return chunk
            
            # Get unique, non-null postal codes
            uniq_postal = chunk['postal_code'].dropna().drop_duplicates()
            
            if uniq_postal.empty:
                logger.warning("No postal codes found for geocoding")
                return chunk
            
            # Build geocoded results
            results = []
            for pc in uniq_postal:
                res = self._census_geocode_postal(str(pc))
                res["postal_code"] = pc
                results.append(res)
            
            postal_lookup = pd.DataFrame(results)
            
            # Merge geocoded data back to chunk
            return chunk.merge(postal_lookup, on="postal_code", how="left")
        
        return self.chunk_processor.filter_chunks(chunks, geocode_chunk)
    
    def _census_geocode_postal(self, postal_code: str) -> dict:
        """Geocode a US postal code using the Census geocoder"""
        url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
        params = {
            "address": postal_code,
            "benchmark": "Public_AR_Current",
            "format": "json"
        }
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            matches = r.json().get("result", {}).get("addressMatches", [])
            if matches:
                c = matches[0]["coordinates"]
                prov_lat = c["y"]
                prov_lng = c["x"]
                # Try to get CBSA from the geographies if available
                cbsa = None
                geos = matches[0].get("geographies", {})
                # CBSA may be under "Census Blocks" or "Metropolitan Statistical Areas"
                if "Census Blocks" in geos and geos["Census Blocks"]:
                    cbsa = geos["Census Blocks"][0].get("CBSA", None)
                if not cbsa and "Metropolitan Statistical Areas" in geos and geos["Metropolitan Statistical Areas"]:
                    cbsa = geos["Metropolitan Statistical Areas"][0].get("CBSA Code", None)
                return {"prov_lat": prov_lat, "prov_lng": prov_lng, "cbsa": cbsa}
        except Exception as e:
            logger.warning(f"Geocoding failed for postal code {postal_code}: {e}")
        return {"prov_lat": None, "prov_lng": None, "cbsa": None}
