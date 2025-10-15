"""
TikTok Marketing API Extractor - Clean Version
"""

import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TikTokExtractor:
    def __init__(self, app_id: str, app_secret: str, access_token: str, advertiser_id: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.advertiser_id = advertiser_id
        self.base_url = "https://business-api.tiktok.com/open_api/v1.3"

    def extract_report_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        all_data = []
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        while start <= end:
            chunk_end = min(start + timedelta(days=29), end)
            s_date = start.strftime("%Y-%m-%d")
            e_date = chunk_end.strftime("%Y-%m-%d")
            logger.info(f"Fetching chunk: {s_date} to {e_date}")

            params = {
                "advertiser_id": self.advertiser_id,
                "report_type": "BASIC",
                "dimensions": '["ad_id","stat_time_day"]',
                "metrics": '["spend","impressions","clicks","ctr","cpm","cpc","reach","frequency","video_play_actions","video_watched_2s","video_watched_6s","average_video_play"]',
                "data_level": "AUCTION_AD",
                "start_date": s_date,
                "end_date": e_date,
                "page_size": 1000,
                "page": 1
            }

            chunk_rows = 0
            while True:
                try:
                    response = requests.get(
                        f"{self.base_url}/report/integrated/get/",
                        headers={"Access-Token": self.access_token},
                        params=params
                    )
                    response.raise_for_status()
                    result = response.json()

                    if result.get("code") != 0:
                        logger.error(f"TikTok API error: {result.get('message')}")
                        break

                    page_data = result.get("data", {}).get("list", [])
                    if not page_data:
                        break
                        
                    all_data.extend(page_data)
                    chunk_rows += len(page_data)

                    page_info = result.get("data", {}).get("page_info", {})
                    total_pages = page_info.get("total_page", 1)
                    
                    if params["page"] >= total_pages:
                        break
                    params["page"] += 1
                    
                except Exception as e:
                    logger.error(f"Error fetching chunk {s_date} to {e_date}: {e}")
                    break

            logger.info(f"Chunk {s_date} to {e_date}: Fetched {chunk_rows} rows")
            start = chunk_end + timedelta(days=1)

        logger.info(f"TOTAL ROWS FETCHED: {len(all_data)}")
        
        if not all_data:
            logger.warning("NO DATA returned from TikTok API")
            return pd.DataFrame()

        ad_ids = [str(row["dimensions"]["ad_id"]) for row in all_data if row.get("dimensions", {}).get("ad_id")]
        logger.info(f"Fetching details for {len(set(ad_ids))} unique ads")
        
        ad_details = self._get_ad_details(list(set(ad_ids)))
        df = self._transform_to_dataframe(all_data, ad_details)
        
        logger.info(f"Final DataFrame: {len(df)} rows")
        return df

    def _get_ad_details(self, ad_ids: List[str]) -> Dict[str, Dict]:
        """Fetch ad details"""
        ad_details = {}
        endpoint = f"{self.base_url}/ad/get/"
        headers = {"Access-Token": self.access_token}

        for i in range(0, len(ad_ids), 100):
            batch_ids = ad_ids[i:i + 100]
            
            params = {
                "advertiser_id": self.advertiser_id,
                "filtering": json.dumps({"ad_ids": batch_ids}),
                "fields": '["ad_id","ad_name","adgroup_id","adgroup_name","campaign_id","campaign_name","ad_text","call_to_action","ad_format","creative_type"]'
            }
            
            try:
                response = requests.get(endpoint, headers=headers, params=params)
                result = response.json()
                
                if result.get("code") == 0:
                    ads = result.get("data", {}).get("list", [])
                    for ad in ads:
                        ad_id = str(ad["ad_id"])
                        ad_details[ad_id] = ad
                        logger.info(f"Got ad: {ad.get('ad_name')} - Campaign: {ad.get('campaign_name')}")
                else:
                    logger.warning(f"Ad details API error: {result.get('message')}")
                    
            except Exception as e:
                logger.warning(f"Failed to fetch ad details batch: {e}")
                
        logger.info(f"Fetched details for {len(ad_details)} ads")
        return ad_details

    def _transform_to_dataframe(self, raw_data: List[Dict], ad_details: Dict[str, Dict]) -> pd.DataFrame:
        """Transform TikTok API response to DataFrame"""
        records = []
        
        for row in raw_data:
            ad_id = str(row.get("dimensions", {}).get("ad_id", ""))
            ad_info = ad_details.get(ad_id, {})
            metrics = row.get("metrics", {})

            video_views = int(metrics.get("video_play_actions", 0))
            video_2s = int(metrics.get("video_watched_2s", 0))
            video_6s = int(metrics.get("video_watched_6s", 0))
            spend = float(metrics.get("spend", 0))
            reach = int(metrics.get("reach", 0))

            format_value = ad_info.get("ad_format", ad_info.get("creative_type", "VIDEO"))

            records.append({
                'DATE': row.get("dimensions", {}).get("stat_time_day"),
                'VIDEO_AVERAGE_PLAY_TIME': float(metrics.get("average_video_play", 0)),
                'FORMAT': format_value,
                'VIDEO_VIEWS_AT_50': video_6s or None,
                'FREQUENCY': float(metrics.get("frequency", 0)),
                'AMOUNT_SPENT_USD': spend,
                'VIDEO_VIEWS_AT_75': int(video_views * 0.75) if video_views else None,
                'VIDEO_VIEWS_AT_25': video_2s or None,
                'CPR': round(spend / reach, 6) if reach else None,
                'REACH': reach,
                'CTR_DESTINATION': float(metrics.get("ctr", 0)),
                'CURRENCY': "USD",
                'IMPRESSIONS': int(metrics.get("impressions", 0)),
                'CPM': float(metrics.get("cpm", 0)),
                'CPC_DESTINATION': float(metrics.get("cpc", 0)),
                'LINK_CLICKS': int(metrics.get("clicks", 0)),
                'CALL_TO_ACTION': ad_info.get("call_to_action", ""),
                'TEXT': ad_info.get("ad_text", ""),
                'PLATFORM': "TikTok",
                'LANGUAGE': "en",
                'CREATIVE': ad_id,
                'AD_NAME': ad_info.get("ad_name", ""),
                'VIDEO_VIEWS_AT_100': video_views if video_views else None,
                'VIDEO_VIEWS': video_views,
                'AD_GROUP_NAME': ad_info.get("adgroup_name", ""),
                'CAMPAIGN_NAME': ad_info.get("campaign_name", "")
            })

        df = pd.DataFrame(records)
        if not df.empty:
            df['DATE'] = pd.to_datetime(df['DATE'])
        return df


class DataTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform data"""
        if df.empty:
            return df
            
        df = df.fillna({
            'VIDEO_AVERAGE_PLAY_TIME': 0.0,
            'FORMAT': 'VIDEO',
            'FREQUENCY': 0.0,
            'AMOUNT_SPENT_USD': 0.0,
            'REACH': 0,
            'CTR_DESTINATION': 0.0,
            'IMPRESSIONS': 0,
            'CPM': 0.0,
            'CPC_DESTINATION': 0.0,
            'LINK_CLICKS': 0,
            'CALL_TO_ACTION': '',
            'TEXT': '',
            'CREATIVE': '',
            'AD_NAME': '',
            'VIDEO_VIEWS': 0,
            'AD_GROUP_NAME': '',
            'CAMPAIGN_NAME': '',
        })
        return df


class BigQueryLoader:
    def __init__(self, project_id: str, dataset_id: str, credentials_path: str):
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def delete_existing_dates(self, df: pd.DataFrame, table_name: str = "TIKTOKREPORT_RAW"):
        """Delete existing data for dates being loaded (prevents duplicates)"""
        if df.empty:
            return
            
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        dates = df['DATE'].dt.strftime('%Y-%m-%d').unique()
        dates_str = "', '".join(dates)
        query = f"DELETE FROM `{table_id}` WHERE DATE IN ('{dates_str}')"
        
        try:
            self.client.query(query).result()
            print(f"Deleted existing data for {len(dates)} dates")
        except Exception as e:
            if "Not found" not in str(e):
                print(f"Warning deleting existing dates: {e}")

    def load_to_bigquery(self, df: pd.DataFrame, table_name: str = "TIKTOKREPORT_RAW"):
        """Load data to BigQuery"""
        if df.empty:
            print("No data to load to BigQuery")
            return
            
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Delete existing dates first
        self.delete_existing_dates(df, table_name)
        
        # Load new data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("DATE", "DATE"),
                bigquery.SchemaField("VIDEO_AVERAGE_PLAY_TIME", "FLOAT"),
                bigquery.SchemaField("FORMAT", "STRING"),
                bigquery.SchemaField("VIDEO_VIEWS_AT_50", "INTEGER"),
                bigquery.SchemaField("FREQUENCY", "FLOAT"),
                bigquery.SchemaField("AMOUNT_SPENT_USD", "FLOAT"),
                bigquery.SchemaField("VIDEO_VIEWS_AT_75", "INTEGER"),
                bigquery.SchemaField("VIDEO_VIEWS_AT_25", "INTEGER"),
                bigquery.SchemaField("CPR", "FLOAT"),
                bigquery.SchemaField("REACH", "INTEGER"),
                bigquery.SchemaField("CTR_DESTINATION", "FLOAT"),
                bigquery.SchemaField("CURRENCY", "STRING"),
                bigquery.SchemaField("IMPRESSIONS", "INTEGER"),
                bigquery.SchemaField("CPM", "FLOAT"),
                bigquery.SchemaField("CPC_DESTINATION", "FLOAT"),
                bigquery.SchemaField("LINK_CLICKS", "INTEGER"),
                bigquery.SchemaField("CALL_TO_ACTION", "STRING"),
                bigquery.SchemaField("TEXT", "STRING"),
                bigquery.SchemaField("PLATFORM", "STRING"),
                bigquery.SchemaField("LANGUAGE", "STRING"),
                bigquery.SchemaField("CREATIVE", "STRING"),
                bigquery.SchemaField("AD_NAME", "STRING"),
                bigquery.SchemaField("VIDEO_VIEWS_AT_100", "INTEGER"),
                bigquery.SchemaField("VIDEO_VIEWS", "INTEGER"),
                bigquery.SchemaField("AD_GROUP_NAME", "STRING"),
                bigquery.SchemaField("CAMPAIGN_NAME", "STRING"),
            ]
        )
        
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"Loaded {len(df)} rows to BigQuery")
        
        # Show summary
        table = self.client.get_table(table_id)
        print(f"Table now has {table.num_rows} total rows")


def run_etl_pipeline(app_id, app_secret, access_token, advertiser_id, 
                     project_id, dataset_id, credentials_path, 
                     start_date, end_date):
    """Run the complete ETL pipeline"""
    print("="*60)
    print("TikTok ETL Pipeline Starting")
    print("="*60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Advertiser ID: {advertiser_id}")
    print(f"Target: {project_id}.{dataset_id}.TIKTOKREPORT_RAW")
    print("="*60)
    
    # Extract
    extractor = TikTokExtractor(app_id, app_secret, access_token, advertiser_id)
    raw_data = extractor.extract_report_data(start_date, end_date)
    
    if raw_data.empty:
        print("ERROR: No data extracted - check logs above")
        return
    
    # Transform
    transformer = DataTransformer()
    transformed_data = transformer.transform(raw_data)
    
    # Load
    loader = BigQueryLoader(project_id, dataset_id, credentials_path)
    loader.load_to_bigquery(transformed_data)
    
    print("="*60)
    print("Pipeline Completed Successfully")
    print("="*60)


if __name__ == "__main__":
    # YOUR CREDENTIALS
    TIKTOK_APP_ID = "7561256923966750737"
    TIKTOK_APP_SECRET = "01264ebfd0692d7c6556ab59992f2d292440977f"
    TIKTOK_ACCESS_TOKEN = "0417fb1524306e61a8a5d18426d5f6f4daebb5c6"
    TIKTOK_ADVERTISER_ID = "7480171442983141393"

    PROJECT_ID = "slstrategy"
    DATASET_ID = "empower_api_data"
    CREDENTIALS_PATH = "./service-account-key.json"

    # Date range - Full backfill from March 3
    START_DATE = "2025-03-03"
    END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    run_etl_pipeline(
        app_id=TIKTOK_APP_ID,
        app_secret=TIKTOK_APP_SECRET,
        access_token=TIKTOK_ACCESS_TOKEN,
        advertiser_id=TIKTOK_ADVERTISER_ID,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        credentials_path=CREDENTIALS_PATH,
        start_date=START_DATE,
        end_date=END_DATE
    )
