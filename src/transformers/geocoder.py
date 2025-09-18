"""
Geocoding and geographic data enrichment
"""
import pandas as pd
import logging
import json
from typing import Iterator, Set
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import as_completed
from src.utils.chunk_processor import ChunkProcessor
from src.utils.api_clients import CensusGeocoderClient, HUDClient, NPPESClient

logger = logging.getLogger(__name__)

class GeocodingTransformer:
    """Add geographic and provider enrichment data"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
        self.census_client = CensusGeocoderClient()
        self.hud_client = HUDClient()
        self.nppes_client = NPPESClient()
        
        # Cache for lookups to avoid duplicate API calls
        self.postal_lookup_cache = {}
        self.npi_lookup_cache = {}
    
    def enrich_with_nppes_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Enrich chunks with NPPES provider data using the specified NPPES lookup logic"""
        logger.info("Enriching with NPPES data...")
        
        # First, collect all unique NPIs across chunks
        unique_npis = set()
        chunk_list = []
        
        for chunk in chunks:
            logger.info(f"NPPES enrich: Processing chunk with {len(chunk)} rows")
            if 'prov_npi' in chunk.columns:
                chunk_npis = self._get_unique_npis(chunk, 'prov_npi')
                unique_npis.update(chunk_npis)
                logger.info(f"NPPES enrich: Found {len(chunk_npis)} unique NPIs in chunk")
            chunk_list.append(chunk)
        
        # Perform memory-efficient NPPES lookup
        if unique_npis:
            logger.info(f"Looking up {len(unique_npis)} unique NPIs using memory-efficient NPPES API")
            try:
                # Use the memory-efficient NPPES lookup function
                nppes_data = self._memory_efficient_nppes_lookup(list(unique_npis))
                if not nppes_data.empty:
                    logger.info(f"NPPES lookup successful: {len(nppes_data)} records found")
                else:
                    logger.warning("NPPES lookup returned no records")
            except Exception as e:
                logger.error(f"NPPES lookup failed: {e}")
                nppes_data = pd.DataFrame()
        else:
            logger.warning("No NPIs found for NPPES lookup")
            nppes_data = pd.DataFrame()
        
        # Merge NPPES data with each chunk
        for chunk in chunk_list:
            logger.info(f"NPPES enrich: Yielding chunk with {len(chunk)} rows")
            if 'prov_npi' in chunk.columns and not nppes_data.empty:
                chunk = self._merge_nppes_data(chunk, nppes_data)
                logger.info(f"NPPES enrich: After merge: {len(chunk)} rows")
            yield chunk
    
    def _merge_hud_data(self, chunk: pd.DataFrame, hud_data: pd.DataFrame) -> pd.DataFrame:
        """Merge HUD CBSA data with chunk"""
        if hud_data.empty:
            chunk['cbsa'] = None
            chunk['cbsaname'] = None
            return chunk
            
        return pd.merge(
            chunk,
            hud_data[['zip', 'cbsa', 'cbsaname']],
            left_on='zip5',
            right_on='zip',
            how='left'
        ).drop(columns=['zip'], errors='ignore')
    
    def _get_unique_npis(self, df: pd.DataFrame, npi_col: str) -> Set[str]:
        """Extract unique, cleaned NPIs from dataframe"""
        return set(
            df[npi_col].astype(str)
            .str.replace(r"\D", "", regex=True)
            .dropna()
            .unique()
        )
    
    def _memory_efficient_nppes_lookup(self, npis: list) -> pd.DataFrame:
        """
        Memory-efficient NPPES lookup with batching, caching, and rate limiting.
        Returns a DataFrame with NPPES info for each unique NPI (NPI-1 and NPI-2).
        """
        import requests
        import json
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from pathlib import Path
        
        NPPES_BASE = "https://npiregistry.cms.hhs.gov/api/"
        HEADERS = {"User-Agent": "NPPES-Lookup/1.0 (+your_email@example.com)"}
        
        # Create cache directory
        cache_dir = Path("data/cache")
        cache_dir.mkdir(exist_ok=True)
        cache_file = cache_dir / "nppes_cache.json"
        
        # Load existing cache
        nppes_cache = self._load_nppes_cache(cache_file)
        
        def build_nppes_url(npi):
            """Build the NPPES API URL for a given NPI."""
            return f"{NPPES_BASE}?number={npi}&version=2.1"
        
        def fetch_nppes_record(npi, timeout=15):
            """Fetch a single NPPES record for a given NPI, for any enumeration type in one call."""
            # Check cache first
            if str(npi) in nppes_cache:
                return nppes_cache[str(npi)]
            
            url = build_nppes_url(npi)
            tries = 0
            while True:
                tries += 1
                try:
                    r = requests.get(url, headers=HEADERS, timeout=timeout)
                    if r.status_code == 429:
                        # Rate limited - wait longer
                        wait_time = min(5.0, 1.5 * tries)
                        logger.warning(f"Rate limited for NPI {npi}, waiting {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    r.raise_for_status()
                    payload = r.json()
                    
                    # Check if we got results
                    if not payload.get("result_count"):
                        result = {"npi": str(npi), "enumeration_type": None, "error": "no_results"}
                    else:
                        result = payload
                    
                    # Cache the result
                    nppes_cache[str(npi)] = result
                    return result
                except (requests.RequestException, json.JSONDecodeError) as e:
                    if tries < 4:
                        time.sleep(1.2 * tries)
                        continue
                    return {"npi": str(npi), "error": str(e)}
        
        def parse_nppes_payload(npi, payload):
            """Parse the NPPES API response into a flat dict."""
            out = {
                "npi": str(npi),
                "enumeration_type": None,
                "org_name": None,
                "status": None,
                "primary_taxonomy_code": None,
                "primary_taxonomy_desc": None,
                "address_purpose": None,
                "address_type": None,
                "address_1": None,
                "address_2": None,
                "city": None,
                "state": None,
                "postal_code": None,
                "country_code": None,
                "telephone_number": None,
                "fax_number": None,
                "error": None,
            }
            
            if isinstance(payload, dict) and payload.get("error") == "skip_NPI-1":
                out["enumeration_type"] = payload.get("enumeration_type")
                out["error"] = "skip_NPI-1"
                return out

            if not payload or "error" in payload:
                out["error"] = payload.get("error") if isinstance(payload, dict) else "no_payload"
                return out
            if not payload.get("result_count"):
                out["error"] = "no_results"
                return out

            res = payload["results"][0]
            out["enumeration_type"] = res.get("enumeration_type")
            basic = res.get("basic") or {}
            out["org_name"] = basic.get("organization_name")
            out["status"] = basic.get("status")

            # Address (grab LOCATION if present, else first)
            addrs = res.get("addresses") or []
            loc = next((a for a in addrs if (a.get("address_purpose") or "").upper() == "LOCATION"), addrs[0] if addrs else {})
            out["address_purpose"] = loc.get("address_purpose")
            out["address_type"] = loc.get("address_type")
            out["address_1"] = loc.get("address_1")
            out["address_2"] = loc.get("address_2")
            out["city"] = loc.get("city")
            out["state"] = loc.get("state")
            out["postal_code"] = loc.get("postal_code")
            out["country_code"] = loc.get("country_code")
            out["telephone_number"] = loc.get("telephone_number")
            out["fax_number"] = loc.get("fax_number")

            # Taxonomies: just get the primary if present
            tax = res.get("taxonomies") or []
            prim = next((t for t in tax if t.get("primary")), None)
            if prim:
                out["primary_taxonomy_code"] = prim.get("code")
                out["primary_taxonomy_desc"] = prim.get("desc")

            return out
        
        # Process NPIs with batching and rate limiting
        results = []
        successful_lookups = 0
        failed_lookups = 0
        
        # Batch NPIs to avoid overwhelming the API
        batch_size = 50  # Process in smaller batches
        npi_batches = [npis[i:i + batch_size] for i in range(0, len(npis), batch_size)]
        
        logger.info(f"Processing {len(npis)} NPIs in {len(npi_batches)} batches of {batch_size}")
        
        for batch_idx, npi_batch in enumerate(npi_batches):
            logger.info(f"Processing batch {batch_idx + 1}/{len(npi_batches)} with {len(npi_batch)} NPIs")
            
            # Use fewer workers to avoid rate limiting
            with ThreadPoolExecutor(max_workers=5) as ex:
                futs = {ex.submit(fetch_nppes_record, npi): npi for npi in npi_batch}
                
                for fut in as_completed(futs):
                    npi = futs[fut]
                    payload = fut.result()
                    record = parse_nppes_payload(npi, payload)
                    results.append(record)
                    
                    # Track success/failure
                    if record.get("error"):
                        failed_lookups += 1
                    else:
                        successful_lookups += 1
            
            # Add delay between batches to respect rate limits
            if batch_idx < len(npi_batches) - 1:
                time.sleep(2)  # 2 second delay between batches
        
        # Save cache
        self._save_nppes_cache(nppes_cache, cache_file)
        
        lookup = pd.DataFrame(results)
        logger.info(f"NPPES lookup completed: {successful_lookups} successful, {failed_lookups} failed")
        return lookup.reset_index(drop=True)
    
    def _load_nppes_cache(self, cache_file: Path) -> dict:
        """Load NPPES cache from file"""
        try:
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load NPPES cache: {e}")
        return {}
    
    def _save_nppes_cache(self, cache: dict, cache_file: Path) -> None:
        """Save NPPES cache to file"""
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache, f, indent=2)
            logger.info(f"NPPES cache saved with {len(cache)} entries")
        except Exception as e:
            logger.warning(f"Failed to save NPPES cache: {e}")
    
    def _nppes_lookup_for_column_data(self, npis: list) -> pd.DataFrame:
        """
        Legacy NPPES lookup function - redirects to memory-efficient version.
        """
        return self._memory_efficient_nppes_lookup(npis)
    
    def _merge_nppes_data(self, chunk: pd.DataFrame, nppes_data: pd.DataFrame) -> pd.DataFrame:
        """Merge NPPES data with chunk"""
        return pd.merge(
            chunk, 
            nppes_data, 
            left_on='prov_npi', 
            right_on='npi', 
            how='left'  # Changed from 'inner' to 'left' to preserve all rows
        ).drop(columns=['npi', 'error'], errors='ignore')
    
    def add_geocoding(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Add geocoding data (lat/lng/CBSA) to chunks"""
        logger.info("Adding geocoding data...")
        
        # Collect unique postal codes
        unique_postal_codes = set()
        chunk_list = []
        
        for chunk in chunks:
            if 'postal_code' in chunk.columns:
                postal_codes = chunk['postal_code'].dropna().astype(str).unique()
                unique_postal_codes.update(postal_codes)
            chunk_list.append(chunk)
        
        # Perform bulk geocoding
        if unique_postal_codes:
            logger.info(f"Geocoding {len(unique_postal_codes)} unique postal codes")
            geocoded_data = self.census_client.bulk_geocode(list(unique_postal_codes))
            self.postal_lookup_cache = geocoded_data.set_index('postal_code').to_dict('index')
        
        # Merge geocoding data with chunks
        for chunk in chunk_list:
            if 'postal_code' in chunk.columns and self.postal_lookup_cache:
                chunk = self._merge_geocoding_data(chunk, geocoded_data)
            yield chunk
    
    def _merge_geocoding_data(self, chunk: pd.DataFrame, geocoded_data: pd.DataFrame) -> pd.DataFrame:
        """Merge geocoding data with chunk"""
        return pd.merge(
            chunk,
            geocoded_data,
            on='postal_code',
            how='left'
        )
    
    def address_to_latlon_cbsa(self, address_1: str, address_2: str = None, city: str = None, state: str = None, postal_code: str = None) -> dict:
        """
        Geocode an address using Census API and return lat/lon/CBSA information.
        
        Args:
            address_1: Primary address line
            address_2: Secondary address line (optional)
            city: City name
            state: State abbreviation
            postal_code: ZIP code
            
        Returns:
            dict: Contains lat, lon, cbsa (with cbsa_geoid and cbsa_name), and error if any
        """
        import requests
        import pandas as pd
        
        # Build the full address string in the required format: address_1, city, state postal_code
        if not (address_1 and city and state and postal_code):
            return {"error": "missing_required_address_components"}
        
        # Check for NaN values
        if not (pd.notna(address_1) and pd.notna(city) and pd.notna(state) and pd.notna(postal_code)):
            return {"error": "missing_required_address_components"}
        
        # Construct address in the specific format: address_1, city, state postal_code
        # Trim postal code to 5 digits to avoid invalid ZIP codes
        postal_code_clean = str(postal_code).strip()[:5]
        full_address = f"{str(address_1).strip()}, {str(city).strip()}, {str(state).strip()} {postal_code_clean}"
        
        base = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
        params = {
            "address": full_address,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",             # required for geographies
            # CBSA layers (either names or IDs 93=Metro, 91=Micro)
            "layers": "Metropolitan Statistical Areas,Micropolitan Statistical Areas",
            "format": "json",
        }
        
        try:
            j = requests.get(base, params=params, timeout=30).json()
            matches = j.get("result", {}).get("addressMatches", [])
            if not matches:
                return {"error": "no_match"}

            m = matches[0]
            lon, lat = m["coordinates"]["x"], m["coordinates"]["y"]

            # Try CBSA directly from this response
            geos = j["result"].get("geographies", {})
            msa = geos.get("Metropolitan Statistical Areas", []) or geos.get("Micropolitan Statistical Areas", [])

            # If CBSA not present, fall back: look up by coordinates
            if not msa:
                coord_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
                coord_params = {
                    "x": lon, "y": lat,
                    "benchmark": "Public_AR_Current",
                    "vintage": "Current_Current",
                    "layers": "93,91",   # Metro, Micro by ID
                    "format": "json",
                }
                j2 = requests.get(coord_url, params=coord_params, timeout=30).json()
                geos2 = j2.get("result", {}).get("geographies", {})
                msa = geos2.get("Metropolitan Statistical Areas", []) or geos2.get("Micropolitan Statistical Areas", [])

            cbsa = None
            if msa:
                cbsa = {"cbsa_geoid": msa[0]["GEOID"], "cbsa_name": msa[0]["NAME"]}

            return {"lat": lat, "lon": lon, "cbsa": cbsa, "error": None}
            
        except Exception as e:
            logger.error(f"Geocoding failed for address '{full_address}': {e}")
            return {"error": str(e)}
    
    def enrich_with_census_geocoding(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """
        Enrich chunks with census geocoding data using address components from NPPES.
        Adds lat, lon, cbsa_geoid, and cbsa_name columns.
        
        This implementation is optimized to:
        1. Avoid duplicate API calls for identical addresses
        2. Cache results across chunks
        3. Handle failures gracefully
        4. Skip geocoding for rows with missing required data
        """
        logger.info("Enriching with census geocoding data...")
        
        # Cache for geocoding results to avoid duplicate API calls
        geocoding_cache = {}
        
        def geocode_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            """Geocode a single chunk using address components with smart caching"""
            if not all(col in chunk.columns for col in ['address_1', 'city', 'state', 'postal_code']):
                logger.warning("Missing required address columns for geocoding")
                return chunk
            
            # Initialize geocoding columns
            chunk['lat'] = None
            chunk['lon'] = None
            chunk['cbsa_geoid'] = None
            chunk['cbsa_name'] = None
            chunk['geocoding_error'] = None
            
            # Counters for this chunk
            total_cached = 0
            total_api_calls = 0
            
            # Create address key for each row and group by unique addresses
            def create_address_key(row):
                """Create a unique key for address geocoding using the format: address_1, city, state postal_code"""
                address_1 = str(row.get('address_1', '')).strip() if pd.notna(row.get('address_1')) else ''
                city = str(row.get('city', '')).strip() if pd.notna(row.get('city')) else ''
                state = str(row.get('state', '')).strip() if pd.notna(row.get('state')) else ''
                postal_code = str(row.get('postal_code', '')).strip() if pd.notna(row.get('postal_code')) else ''
                
                # Skip if missing required components
                if not (address_1 and city and state and postal_code):
                    return None
                
                # Trim postal code to 5 digits to match the geocoding function
                postal_code_clean = postal_code[:5]
                    
                # Use the same format as the full address: address_1, city, state postal_code
                return f"{address_1}, {city}, {state} {postal_code_clean}"
            
            # Group rows by address key
            address_groups = {}
            for idx, row in chunk.iterrows():
                key = create_address_key(row)
                if key:
                    if key not in address_groups:
                        address_groups[key] = []
                    address_groups[key].append(idx)
                else:
                    # Mark as missing required data
                    chunk.at[idx, 'geocoding_error'] = 'missing_required_address_components'
            
            # Geocode unique addresses only
            for address_key, row_indices in address_groups.items():
                if address_key in geocoding_cache:
                    # Use cached result
                    result = geocoding_cache[address_key]
                    total_cached += len(row_indices)
                    logger.debug(f"Using cached geocoding for {len(row_indices)} rows with address key: {address_key[:50]}...")
                else:
                    # Make API call for new address
                    total_api_calls += 1
                    logger.info(f"Geocoding new address: {address_key[:50]}...")
                    
                    # Parse the address key back into components
                    # Format: "address_1, city, state postal_code"
                    parts = address_key.split(', ')
                    if len(parts) >= 3:
                        address_1 = parts[0].strip()
                        city = parts[1].strip()
                        state_zip = parts[2].strip()
                        # Split state and postal_code
                        state_zip_parts = state_zip.split(' ')
                        if len(state_zip_parts) >= 2:
                            state = state_zip_parts[0].strip()
                            postal_code = ' '.join(state_zip_parts[1:]).strip()
                        else:
                            state = state_zip
                            postal_code = ''
                    else:
                        # Fallback parsing
                        address_1 = address_key
                        city = ''
                        state = ''
                        postal_code = ''
                    
                    result = self.address_to_latlon_cbsa(
                        address_1=address_1,
                        address_2=None,  # Not used in the new format
                        city=city,
                        state=state,
                        postal_code=postal_code
                    )
                    # Cache the result
                    geocoding_cache[address_key] = result
                    logger.info(f"Geocoded result: {result}")
                
                # Apply result to all rows with this address
                for idx in row_indices:
                    if 'error' in result:
                        chunk.at[idx, 'geocoding_error'] = result['error']
                    else:
                        chunk.at[idx, 'lat'] = result.get('lat')
                        chunk.at[idx, 'lon'] = result.get('lon')
                        if result.get('cbsa'):
                            chunk.at[idx, 'cbsa_geoid'] = result['cbsa'].get('cbsa_geoid')
                            chunk.at[idx, 'cbsa_name'] = result['cbsa'].get('cbsa_name')
            
            logger.info(f"Geocoded {len(address_groups)} unique addresses (API calls: {total_api_calls}, cached: {total_cached})")
            return chunk
        
        return self.chunk_processor.filter_chunks(chunks, geocode_chunk)
    
    def add_cbsa_names(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Add CBSA names using HUD API"""
        logger.info("Adding CBSA names...")
        
        # Collect unique ZIP codes
        unique_zips = set()
        chunk_list = []
        
        for chunk in chunks:
            if 'postal_code' in chunk.columns:
                # Normalize to 5-digit ZIPs
                chunk['zip5'] = chunk['postal_code'].astype(str).str.extract(r'(\d{5})', expand=False)
                zip_codes = chunk['zip5'].dropna().unique()
                unique_zips.update(zip_codes)
            chunk_list.append(chunk)
        
        # Perform