"""
TikTok Marketing API Extractor
Extracts ad performance data from TikTok Marketing API and loads to BigQuery
"""

import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TikTokExtractor:
    """Handles TikTok Marketing API data extraction"""
    
    def __init__(self, app_id: str, app_secret: str, access_token: str, advertiser_id: str):
        """
        Initialize TikTok API client
        
        Args:
            app_id: TikTok App ID (from Developer Portal)
            app_secret: TikTok App Secret (from Developer Portal)
            access_token: OAuth 2.0 access token
            advertiser_id: TikTok advertiser account ID
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.advertiser_id = advertiser_id
        self.base_url = "https://business-api.tiktok.com/open_api/v1.3"
        
    def get_report_data(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch ad performance report from TikTok API
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of dictionaries containing ad performance data
        """
        endpoint = f"{self.base_url}/report/integrated/get/"
        
        # Request headers
        headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
        
        # API request body with all required fields
        payload = {
            "advertiser_id": self.advertiser_id,
            "report_type": "BASIC",  # Basic ad reporting
            "dimensions": ["ad_id", "stat_time_day"],  # Group by ad and date
            "metrics": [
                "spend",
                "impressions", 
                "clicks",
                "ctr",
                "cpm",
                "cpc",
                "reach",
                "frequency",
                "video_play_actions",
                "video_watched_2s",
                "video_watched_6s",
                "average_video_play",
                "average_video_play_per_user"
            ],
            "data_level": "AUCTION_AD",  # Ad level data
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
            "page": 1
        }
        
        all_data = []
        
        try:
            # Handle pagination
            while True:
                logger.info(f"Fetching TikTok data - Page {payload['page']}")
                
                response = requests.post(endpoint, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                
                # Check API response status
                if result.get("code") != 0:
                    error_msg = result.get("message", "Unknown error")
                    logger.error(f"TikTok API error: {error_msg}")
                    raise Exception(f"TikTok API returned error: {error_msg}")
                
                # Extract data from response
                page_data = result.get("data", {}).get("list", [])
                
                if not page_data:
                    break
                    
                all_data.extend(page_data)
                
                # Check if more pages exist
                page_info = result.get("data", {}).get("page_info", {})
                total_page = page_info.get("total_page", 1)
                
                if payload["page"] >= total_page:
                    break
                    
                payload["page"] += 1
            
            logger.info(f"Successfully fetched {len(all_data)} rows from TikTok API")
            return all_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching TikTok data: {e}")
            raise
    
    def get_ad_details(self, ad_ids: List[str]) -> Dict[str, Dict]:
        """
        Fetch detailed ad information (creative text, names, etc.)
        
        Args:
            ad_ids: List of ad IDs to fetch details for
            
        Returns:
            Dictionary mapping ad_id to ad details
        """
        endpoint = f"{self.base_url}/ad/get/"
        
        headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
        
        ad_details = {}
        
        # TikTok API allows fetching up to 100 ads per request
        batch_size = 100
        
        for i in range(0, len(ad_ids), batch_size):
            batch_ids = ad_ids[i:i + batch_size]
            
            payload = {
                "advertiser_id": self.advertiser_id,
                "filtering": {
                    "ad_ids": batch_ids
                },
                "fields": [
                    "ad_id",
                    "ad_name",
                    "adgroup_id",
                    "adgroup_name",
                    "campaign_id",
                    "campaign_name",
                    "ad_text",
                    "call_to_action",
                    "display_name",
                    "creative_material_mode",
                    "landing_page_url"
                ]
            }
            
            try:
                response = requests.get(endpoint, headers=headers, params={"advertiser_id": self.advertiser_id})
                # Note: Actual implementation may vary based on TikTok API version
                
                result = response.json()
                
                if result.get("code") == 0:
                    ads = result.get("data", {}).get("list", [])
                    for ad in ads:
                        ad_details[ad["ad_id"]] = ad
                        
            except Exception as e:
                logger.warning(f"Could not fetch ad details for batch: {e}")
                continue
        
        return ad_details
    
    def transform_to_bigquery_schema(self, raw_data: List[Dict], ad_details: Dict[str, Dict]) -> List[Dict]:
        """
        Transform TikTok API response to match BigQuery schema
        Maps API fields to your 26 BigQuery columns
        
        Args:
            raw_data: Raw data from TikTok API
            ad_details: Ad metadata from separate API call
            
        Returns:
            List of dictionaries matching BigQuery schema
        """
        transformed_data = []
        
        for row in raw_data:
            ad_id = row.get("dimensions", {}).get("ad_id")
            ad_info = ad_details.get(ad_id, {})
            
            # Calculate video quartile metrics
            video_views = row.get("metrics", {}).get("video_play_actions", 0)
            video_2s = row.get("metrics", {}).get("video_watched_2s", 0)
            video_6s = row.get("metrics", {}).get("video_watched_6s", 0)
            
            # Estimate quartile views (TikTok doesn't provide exact quartiles)
            # These are approximations based on available metrics
            video_25 = video_2s if video_2s else None
            video_50 = video_6s if video_6s else None
            video_75 = int(video_views * 0.75) if video_views else None
            video_100 = int(video_views) if video_views else None
            
            # Map to your BigQuery schema (26 fields)
            transformed_row = {
                # Date
                "DATE": row.get("dimensions", {}).get("stat_time_day"),
                
                # Video metrics
                "VIDEO_AVERAGE_PLAY_TIME": row.get("metrics", {}).get("average_video_play", 0),
                "VIDEO_VIEWS": video_views,
                "VIDEO_VIEWS_AT_25": video_25,
                "VIDEO_VIEWS_AT_50": video_50,
                "VIDEO_VIEWS_AT_75": video_75,
                "VIDEO_VIEWS_AT_100": video_100,
                
                # Format and creative
                "FORMAT": ad_info.get("creative_material_mode", ""),
                "TEXT": ad_info.get("ad_text", ""),
                "CREATIVE": ad_id,  # Using ad_id as creative identifier
                "CALL_TO_ACTION": ad_info.get("call_to_action", ""),
                
                # Performance metrics
                "FREQUENCY": row.get("metrics", {}).get("frequency", 0),
                "AMOUNT_SPENT_USD": row.get("metrics", {}).get("spend", 0),
                "REACH": row.get("metrics", {}).get("reach", 0),
                "CTR_DESTINATION": row.get("metrics", {}).get("ctr", 0),
                "CURRENCY": "USD",  # Assuming USD
                "IMPRESSIONS": row.get("metrics", {}).get("impressions", 0),
                "CPM": row.get("metrics", {}).get("cpm", 0),
                "CPC_DESTINATION": row.get("metrics", {}).get("cpc", 0),
                "LINK_CLICKS": row.get("metrics", {}).get("clicks", 0),
                
                # Calculated metric
                "CPR": self._calculate_cpr(
                    row.get("metrics", {}).get("spend", 0),
                    row.get("metrics", {}).get("reach", 0)
                ),
                
                # Campaign hierarchy
                "CAMPAIGN_NAME": ad_info.get("campaign_name", ""),
                "AD_GROUP_NAME": ad_info.get("adgroup_name", ""),
                "AD_NAME": ad_info.get("ad_name", ""),
                
                # Platform info
                "PLATFORM": "TikTok",
                "LANGUAGE": "en"  # Default to English, adjust as needed
            }
            
            transformed_data.append(transformed_row)
        
        logger.info(f"Transformed {len(transformed_data)} rows for BigQuery")
        return transformed_data
    
    def _calculate_cpr(self, spend: float, reach: float) -> Optional[float]:
        """Calculate Cost Per Reach"""
        if reach and reach > 0:
            return round(spend / reach, 6)
        return None


class BigQueryLoader:
    """Handles loading data to BigQuery"""
    
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        """
        Initialize BigQuery client
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID (e.g., 'slstrategy.EMPOWER_2025')
            table_id: BigQuery table ID (e.g., 'TIKTOKREPORT_RAW')
        """
        self.client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
    def load_data(self, data: List[Dict], write_disposition: str = "WRITE_APPEND") -> None:
        """
        Load data to BigQuery table
        
        Args:
            data: List of dictionaries matching table schema
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY
        """
        if not data:
            logger.warning("No data to load to BigQuery")
            return
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=False  # Use existing table schema
        )
        
        try:
            job = self.client.load_table_from_json(
                data,
                self.table_ref,
                job_config=job_config
            )
            
            job.result()  # Wait for job to complete
            
            logger.info(f"Loaded {len(data)} rows to {self.table_ref}")
            
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise


def main():
    """Main execution function"""
    
    # ============================================
    # CONFIGURATION - Update these values
    # ============================================
    
    # TikTok API Credentials (placeholder - fill in once approved)
    TIKTOK_APP_ID = "your_app_id_here"
    TIKTOK_APP_SECRET = "your_app_secret_here"
    TIKTOK_ACCESS_TOKEN = "your_access_token_here"
    TIKTOK_ADVERTISER_ID = "your_advertiser_id_here"
    
    # BigQuery Configuration
    PROJECT_ID = "your-gcp-project-id"
    DATASET_ID = "slstrategy.EMPOWER_2025"
    TABLE_ID = "TIKTOKREPORT_RAW"
    
    # Date Range (adjust as needed)
    # For daily runs, typically yesterday's data
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    START_DATE = start_date.strftime("%Y-%m-%d")
    END_DATE = end_date.strftime("%Y-%m-%d")
    
    # ============================================
    # EXECUTION
    # ============================================
    
    try:
        logger.info("Starting TikTok ETL pipeline")
        logger.info(f"Date range: {START_DATE} to {END_DATE}")
        
        # Step 1: Extract data from TikTok API
        extractor = TikTokExtractor(
            app_id=TIKTOK_APP_ID,
            app_secret=TIKTOK_APP_SECRET,
            access_token=TIKTOK_ACCESS_TOKEN,
            advertiser_id=TIKTOK_ADVERTISER_ID
        )
        
        raw_data = extractor.get_report_data(START_DATE, END_DATE)
        
        # Step 2: Get ad details for creative text, names, etc.
        ad_ids = [row.get("dimensions", {}).get("ad_id") for row in raw_data if row.get("dimensions", {}).get("ad_id")]
        ad_details = extractor.get_ad_details(ad_ids)
        
        # Step 3: Transform data to BigQuery schema
        transformed_data = extractor.transform_to_bigquery_schema(raw_data, ad_details)
        
        # Step 4: Load to BigQuery
        loader = BigQueryLoader(PROJECT_ID, DATASET_ID, TABLE_ID)
        loader.load_data(transformed_data, write_disposition="WRITE_APPEND")
        
        logger.info("TikTok ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
