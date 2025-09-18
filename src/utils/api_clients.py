"""
API client utilities for external data sources
"""
import requests
import time
import json
import logging
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
from config.settings import APIConfig

logger = logging.getLogger(__name__)

class NPPESClient:
    """Client for NPPES API lookups"""
    
    def __init__(self, max_workers: int = None):
        self.base_url = APIConfig.NPPES_BASE_URL
        self.headers = APIConfig.NPPES_HEADERS
        self.max_workers = max_workers or APIConfig.NPPES_MAX_WORKERS
        self.timeout = APIConfig.REQUEST_TIMEOUT
        
    def build_url(self, npi: str) -> str:
        return f"{self.base_url}?number={npi}&version=2.1"
    
    def get_enumeration_type(self, npi: str) -> Optional[str]:
        """Check enumeration type for NPI (NPI-1 or NPI-2)"""
        url = self.build_url(npi)
        
        for attempt in range(APIConfig.RETRY_ATTEMPTS):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                
                if response.status_code == 429:
                    time.sleep(APIConfig.RATE_LIMIT_DELAY * (attempt + 1))
                    continue
                    
                response.raise_for_status()
                payload = response.json()
                
                if payload.get("result_count"):
                    return payload["results"][0].get("enumeration_type")
                    
            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Attempt {attempt + 1} failed for NPI {npi}: {e}")
                if attempt < APIConfig.RETRY_ATTEMPTS - 1:
                    time.sleep(APIConfig.RATE_LIMIT_DELAY * (attempt + 1))
        
        return None
    
    def fetch_npi_record(self, npi: str) -> Dict:
        """Fetch complete NPI record for any enumeration type in a single API call"""
        url = self.build_url(npi)
        
        for attempt in range(APIConfig.RETRY_ATTEMPTS):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                
                if response.status_code == 429:
                    time.sleep(APIConfig.RATE_LIMIT_DELAY * (attempt + 1))
                    continue
                    
                response.raise_for_status()
                payload = response.json()
                
                # Check if we got results
                if not payload.get("result_count"):
                    return {"npi": str(npi), "enumeration_type": None, "error": "no_results"}
                
                return payload
                
            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Attempt {attempt + 1} failed for NPI {npi}: {e}")
                if attempt < APIConfig.RETRY_ATTEMPTS - 1:
                    time.sleep(APIConfig.RATE_LIMIT_DELAY * (attempt + 1))
        
        return {"npi": str(npi), "error": "fetch_failed"}
    
    def parse_npi_response(self, npi: str, payload: Dict) -> Dict:
        """Parse NPPES API response into structured data"""
        result = {
            "npi": str(npi),
            "enumeration_type": None,
            "org_name": None,
            "status": None,
            "primary_taxonomy_code": None,
            "primary_taxonomy_desc": None,
            "address_1": None,
            "city": None,
            "state": None,
            "postal_code": None,
            "telephone_number": None,
            "error": None,
        }
        
        if payload.get("error") == "skip_NPI-1":
            result.update(payload)
            return result
        
        if not payload or "error" in payload:
            result["error"] = payload.get("error", "no_payload")
            return result
            
        if not payload.get("result_count"):
            result["error"] = "no_results"
            return result
        
        data = payload["results"][0]
        result["enumeration_type"] = data.get("enumeration_type")
        
        # Basic info
        basic = data.get("basic", {})
        result["org_name"] = basic.get("organization_name")
        result["status"] = basic.get("status")
        
        # Address (prefer LOCATION, fallback to first address)
        addresses = data.get("addresses", [])
        if addresses:
            location_addr = next(
                (addr for addr in addresses if addr.get("address_purpose", "").upper() == "LOCATION"),
                addresses[0]
            )
            result.update({
                "address_1": location_addr.get("address_1"),
                "city": location_addr.get("city"),
                "state": location_addr.get("state"),
                "postal_code": location_addr.get("postal_code"),
                "telephone_number": location_addr.get("telephone_number"),
            })
        
        # Primary taxonomy
        taxonomies = data.get("taxonomies", [])
        primary_tax = next((tax for tax in taxonomies if tax.get("primary")), None)
        if primary_tax:
            result["primary_taxonomy_code"] = primary_tax.get("code")
            result["primary_taxonomy_desc"] = primary_tax.get("desc")
        
        return result
    
    def bulk_lookup(self, npis: List[str]) -> pd.DataFrame:
        """Perform bulk NPI lookups with threading and batching"""
        results = []
        successful_lookups = 0
        failed_lookups = 0
        
        logger.info(f"Starting NPPES lookup for {len(npis)} NPIs with {self.max_workers} workers...")
        
        # Process NPIs in batches to avoid overwhelming the API
        batch_size = APIConfig.NPPES_BATCH_SIZE
        total_batches = (len(npis) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(npis))
            batch_npis = npis[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_npis)} NPIs)")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self.fetch_npi_record, npi): npi for npi in batch_npis}
                
                progress_bar = tqdm(
                    as_completed(futures), 
                    total=len(futures), 
                    desc=f"NPPES Batch {batch_num + 1}/{total_batches}",
                    unit="NPI",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
                )
                
                for future in progress_bar:
                    npi = futures[future]
                    try:
                        payload = future.result()
                        record = self.parse_npi_response(npi, payload)
                        results.append(record)
                        
                        # Track success/failure
                        if record.get("error"):
                            failed_lookups += 1
                        else:
                            successful_lookups += 1
                        
                        # Update progress bar description with stats
                        progress_bar.set_description(
                            f"Batch {batch_num + 1} (✓{successful_lookups} ✗{failed_lookups})"
                        )
                        
                    except Exception as e:
                        logger.error(f"Error processing NPI {npi}: {e}")
                        results.append({"npi": npi, "error": str(e)})
                        failed_lookups += 1
                        progress_bar.set_description(
                            f"Batch {batch_num + 1} (✓{successful_lookups} ✗{failed_lookups})"
                        )
            
            # Small delay between batches to be respectful to the API
            if batch_num < total_batches - 1:
                time.sleep(0.5)
        
        logger.info(f"NPPES lookup complete: {successful_lookups} successful, {failed_lookups} failed")
        
        df = pd.DataFrame(results)
        # Return all enumeration types (NPI-1 and NPI-2)
        return df.reset_index(drop=True)


class CensusGeocoderClient:
    """Client for Census geocoding API"""
    
    def __init__(self):
        self.base_url = APIConfig.CENSUS_GEOCODE_URL
        self.timeout = APIConfig.REQUEST_TIMEOUT
    
    def geocode_postal_code(self, postal_code: str) -> Dict:
        """Geocode a postal code to lat/lng/CBSA"""
        params = {
            "address": postal_code,
            "benchmark": "Public_AR_Current",
            "format": "json"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            matches = response.json().get("result", {}).get("addressMatches", [])
            if matches:
                coords = matches[0]["coordinates"]
                
                # Extract CBSA if available
                cbsa = None
                geographies = matches[0].get("geographies", {})
                
                if "Census Blocks" in geographies and geographies["Census Blocks"]:
                    cbsa = geographies["Census Blocks"][0].get("CBSA")
                
                return {
                    "prov_lat": coords["y"],
                    "prov_lng": coords["x"],
                    "cbsa": cbsa
                }
        
        except Exception as e:
            logger.warning(f"Geocoding failed for {postal_code}: {e}")
        
        return {"prov_lat": None, "prov_lng": None, "cbsa": None}
    
    def bulk_geocode(self, postal_codes: List[str]) -> pd.DataFrame:
        """Bulk geocode postal codes"""
        results = []
        
        for postal_code in tqdm(postal_codes, desc="Geocoding postal codes"):
            result = self.geocode_postal_code(postal_code)
            result["postal_code"] = postal_code
            results.append(result)
        
        return pd.DataFrame(results)


class HUDClient:
    """Client for HUD USPS Crosswalk API"""
    
    def __init__(self):
        self.base_url = APIConfig.HUD_URL
        self.headers = {"Authorization": f"Bearer {APIConfig.HUD_TOKEN}"}
        self.timeout = APIConfig.REQUEST_TIMEOUT
    
    def fetch_zip_to_cbsa(self, zip_code: str) -> List[Dict]:
        """Fetch ZIP to CBSA mapping from HUD API"""
        params = {"type": 3, "query": zip_code}  # type 3 = ZIP to CBSA
        
        try:
            response = requests.get(self.base_url, headers=self.headers, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            return response.json().get("data", {}).get("results", [])
            
        except Exception as e:
            logger.warning(f"HUD lookup failed for {zip_code}: {e}")
            return []
    
    def bulk_lookup_cbsa(self, zip_codes: List[str]) -> pd.DataFrame:
        """Bulk lookup CBSA names for ZIP codes"""
        all_results = []
        
        for i, zip_code in enumerate(tqdm(zip_codes, desc="HUD CBSA lookup")):
            results = self.fetch_zip_to_cbsa(zip_code)
            all_results.extend(results)
            
            # Rate limiting
            if i % 50 == 0 and i > 0:
                time.sleep(1)
        
        if not all_results:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_results)
        
        # If multiple CBSA entries per ZIP, keep the one with highest ratio
        if "tot_ratio" in df.columns:
            df["tot_ratio"] = pd.to_numeric(df["tot_ratio"], errors="coerce").fillna(0)
            df = df.sort_values(["zip", "tot_ratio"], ascending=[True, False])
            df = df.drop_duplicates("zip")
        
        return df[["zip", "cbsa", "cbsaname"]] if "cbsaname" in df.columns else df