import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
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
        all_chunks = []
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        while current_start <= end_date_dt:
            current_end = min(current_start + timedelta(days=29), end_date_dt)
            chunk_start = current_start.strftime("%Y-%m-%d")
            chunk_end = current_end.strftime("%Y-%m-%d")
            print(f"Fetching chunk: {chunk_start} → {chunk_end}")

            endpoint = f"{self.base_url}/report/integrated/get/"
            headers = {"Access-Token": self.access_token}
            params = {
                "advertiser_id": self.advertiser_id,
                "report_type": "BASIC",
                "dimensions": '["campaign_id","adgroup_id","ad_id","stat_time_day"]',
                "metrics": '["spend","impressions","clicks","ctr","cpm","cpc","reach","frequency","video_play_actions","video_watched_2s","video_watched_6s","average_video_play"]',
                "data_level": "AUCTION_AD",
                "start_date": chunk_start,
                "end_date": chunk_end,
                "page_size": 1000,
                "page": 1
            }

            chunk_data = []

            try:
                while True:
                    logger.info(f"Fetching TikTok data - Page {params['page']} for {chunk_start} → {chunk_end}")
                    response = requests.get(endpoint, headers=headers, params=params)
                    response.raise_for_status()
                    result = response.json()

                    if result.get("code") != 0:
                        error_msg = result.get("message", "Unknown error")
                        logger.error(f"TikTok API error: {error_msg}")
                        raise Exception(f"TikTok API returned error: {error_msg}")

                    page_data = result.get("data", {}).get("list", [])
                    if not page_data:
                        break

                    chunk_data.extend(page_data)

                    page_info = result.get("data", {}).get("page_info", {})
                    total_page = page_info.get("total_page", 1)
                    if params["page"] >= total_page:
                        break
                    params["page"] += 1

                if chunk_data:
                    all_chunks.extend(chunk_data)
                    logger.info(f"Fetched {len(chunk_data)} rows for {chunk_start} → {chunk_end}")

            except Exception as e:
                logger.error(f"Error fetching chunk {chunk_start} → {chunk_end}: {e}")

            current_start = current_end + timedelta(days=1)

        if not all_chunks:
            print("No data returned from TikTok API")
            return pd.DataFrame()

        ad_ids = [row.get("dimensions", {}).get("ad_id") for row in all_chunks if row.get("dimensions", {}).get("ad_id")]
        ad_details = self._get_ad_details(list(set(ad_ids)))
        df = self._transform_to_dataframe(all_chunks, ad_details)
        print(f"Extracted total {len(df)} rows across all chunks")
        return df

    def _get_ad_details(self, ad_ids: List[str]) -> Dict[str, Dict]:
        if not ad_ids:
            return {}
        endpoint = f"{self.base_url}/ad/get/"
        headers = {"Access-Token": self.access_token}
        ad_details = {}
        batch_size = 100
        for i in range(0, len(ad_ids), batch_size):
            batch_ids = ad_ids[i:i + batch_size]
            params = {
                "advertiser_id": self.advertiser_id,
                "filtering": json.dumps({"ad_ids": batch_ids}),
                "fields": '["ad_id","ad_name","adgroup_id","adgroup_name","campaign_id","campaign_name","ad_text","call_to_action","creative_material_mode"]'
            }
            try:
                response = requests.get(endpoint, headers=headers, params=params)
                result = response.json()
                if result.get("code") == 0:
                    ads = result.get("data", {}).get("list", [])
                    for ad in ads:
                        ad_details[str(ad["ad_id"])] = ad
            except Exception as e:
                logger.warning(f"Could not fetch ad details for batch: {e}")
                continue
        return ad_details

    def _transform_to_dataframe(self, raw_data: List[Dict], ad_details: Dict[str, Dict]) -> pd.DataFrame:
        records = []
        for row in raw_data:
            ad_id = str(row.get("dimensions", {}).get("ad_id", ""))
            ad_info = ad_details.get(ad_id, {})
            metrics = row.get("metrics", {})

            try:
                video_views = int(metrics.get("video_play_actions", 0))
            except:
                video_views = 0
            try:
                video_2s = int(metrics.get("video_watched_2s", 0))
            except:
                video_2s = 0
            try:
                video_6s = int(metrics.get("video_watched_6s", 0))
            except:
                video_6s = 0
            try:
                spend = float(metrics.get("spend", 0))
            except:
                spend = 0.0
            try:
                reach = int(metrics.get("reach", 0))
            except:
                reach = 0

            record = {
                'DATE': row.get("dimensions", {}).get("stat_time_day"),
                'VIDEO_AVERAGE_PLAY_TIME': float(metrics.get("average_video_play", 0)),
                'FORMAT': ad_info.get("creative_material_mode", "VIDEO"),
                'VIDEO_VIEWS_AT_50': video_6s if video_6s else None,
                'FREQUENCY': float(metrics.get("frequency", 0)),
                'AMOUNT_SPENT_USD': spend,
                'VIDEO_VIEWS_AT_75': int(video_views * 0.75) if video_views else None,
                'VIDEO_VIEWS_AT_25': video_2s if video_2s else None,
                'CPR': round(spend / reach, 6) if reach > 0 else None,
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
                'CAMPAIGN_NAME': ad_info.get("campaign_name", ""),
            }
            records.append(record)

        df = pd.DataFrame(records)
        df['DATE'] = pd.to_datetime(df['DATE'])
        return df


class DataTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
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
        numeric_columns = {
            'VIDEO_AVERAGE_PLAY_TIME': float,
            'FREQUENCY': float,
            'AMOUNT_SPENT_USD': float,
            'CPR': float,
            'CTR_DESTINATION': float,
            'CPM': float,
            'CPC_DESTINATION': float,
            'REACH': int,
            'IMPRESSIONS': int,
            'LINK_CLICKS': int,
            'VIDEO_VIEWS': int,
        }
        for col, dtype in numeric_columns.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        return df


class BigQueryLoader:
    def __init__(self, project_id: str, dataset_id: str, credentials_path: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        self.client = bigquery.Client(credentials=credentials, project=project_id)

    def delete_existing_dates(self, df: pd.DataFrame, table_name: str = "TIKTOKREPORT_RAW"):
        if df.empty:
            return
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        dates = df['DATE'].dt.strftime('%Y-%m-%d').unique()
        dates_str = "', '".join(dates)
        delete_query = f"DELETE FROM `{table_id}` WHERE DATE IN ('{dates_str}')"
        try:
            self.client.query(delete_query).result()
        except Exception as e:
            if "Not found" not in str(e):
                print(f"Warning deleting existing dates: {e}")

    def load_to_bigquery(self, df: pd.DataFrame, table_name: str = "TIKTOKREPORT_RAW"):
        if df.empty:
            print("No data to load")
            return
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        self.delete_existing_dates(df, table_name)
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
            ],
        )
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


def run_etl_pipeline(app_id, app_secret, access_token, advertiser_id, project_id, dataset_id, credentials_path, start_date, end_date):
    extractor = TikTokExtractor(app_id, app_secret, access_token, advertiser_id)
    raw_data = extractor.extract_report_data(start_date, end_date)
    if raw_data.empty:
        print("No data extracted")
        return
    transformer = DataTransformer()
    transformed_data = transformer.transform(raw_data)
    loader = BigQueryLoader(project_id, dataset_id, credentials_path)
    loader.load_to_bigquery(transformed_data)


if __name__ == "__main__":
    TIKTOK_APP_ID = "7561256923966750737"
    TIKTOK_APP_SECRET = "01264ebfd0692d7c6556ab59992f2d292440977f"
    TIKTOK_ACCESS_TOKEN = "0417fb1524306e61a8a5d18426d5f6f4daebb5c6"
    TIKTOK_ADVERTISER_ID = "7480171442983141393"

    PROJECT_ID = "slstrategy"
    DATASET_ID = "empower_api_data"
    CREDENTIALS_PATH = "./service-account-key.json"

    last_run_file = "./last_run.txt"
    try:
        with open(last_run_file, "r") as f:
            last_run_date = datetime.strptime(f.read().strip(), "%Y-%m-%d")
    except:
        last_run_date = datetime(2025, 3, 3)

    end_date = datetime.now() - timedelta(days=1)
    start_date = last_run_date + timedelta(days=1)

    if start_date > end_date:
        print("No new data to fetch")
    else:
        START_DATE = start_date.strftime("%Y-%m-%d")
        END_DATE = end_date.strftime("%Y-%m-%d")
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
        with open(last_run_file, "w") as f:
            f.write(end_date.strftime("%Y-%m-%d"))
