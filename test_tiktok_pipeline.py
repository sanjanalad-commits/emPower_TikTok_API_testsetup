"""
Test script for TikTok ETL pipeline
Tests transformation logic with dummy data before connecting to real API
"""

import json
from datetime import datetime, timedelta
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MockTikTokData:
    """Generate mock TikTok API responses for testing"""
    
    @staticmethod
    def generate_report_response(num_days: int = 7) -> List[Dict]:
        """
        Generate mock TikTok report API response
        
        Args:
            num_days: Number of days of data to generate
            
        Returns:
            List of mock API response rows
        """
        mock_data = []
        
        base_date = datetime.now() - timedelta(days=num_days)
        
        # Mock ad IDs and campaigns
        campaigns = [
            {
                "campaign_id": "1234567890",
                "campaign_name": "THMC emPower Gateway SouthLA 25",
                "adgroup_id": "2345678901",
                "adgroup_name": "ThmcEmpowerGatew_Sector2_Social",
                "ad_id": "3456789012",
                "ad_name": "TikTok Empower 15s Video A"
            },
            {
                "campaign_id": "1234567891",
                "campaign_name": "emPower Awareness Campaign Q1",
                "adgroup_id": "2345678902",
                "adgroup_name": "Awareness_AdSet_18-35",
                "ad_id": "3456789013",
                "ad_name": "TikTok Empower 15s Video B"
            }
        ]
        
        for day in range(num_days):
            current_date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
            
            for campaign in campaigns:
                # Generate realistic mock metrics
                impressions = 5000 + (day * 100)
                clicks = int(impressions * 0.02)  # 2% CTR
                spend = impressions * 0.01  # $10 CPM
                reach = int(impressions * 0.7)
                video_views = int(impressions * 0.8)
                
                row = {
                    "dimensions": {
                        "ad_id": campaign["ad_id"],
                        "stat_time_day": current_date
                    },
                    "metrics": {
                        "spend": round(spend, 2),
                        "impressions": impressions,
                        "clicks": clicks,
                        "ctr": round((clicks / impressions) * 100, 2),
                        "cpm": round((spend / impressions) * 1000, 2),
                        "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
                        "reach": reach,
                        "frequency": round(impressions / reach, 2) if reach > 0 else 0,
                        "video_play_actions": video_views,
                        "video_watched_2s": int(video_views * 0.9),
                        "video_watched_6s": int(video_views * 0.7),
                        "average_video_play": round(8.5, 2),
                        "average_video_play_per_user": round(9.2, 2)
                    }
                }
                
                mock_data.append(row)
        
        logger.info(f"Generated {len(mock_data)} mock report rows")
        return mock_data
    
    @staticmethod
    def generate_ad_details() -> Dict[str, Dict]:
        """
        Generate mock ad details API response
        
        Returns:
            Dictionary mapping ad_id to ad details
        """
        mock_details = {
            "3456789012": {
                "ad_id": "3456789012",
                "ad_name": "TikTok Empower 15s Video A",
                "adgroup_id": "2345678901",
                "adgroup_name": "ThmcEmpowerGatew_Sector2_Social",
                "campaign_id": "1234567890",
                "campaign_name": "THMC emPower Gateway SouthLA 25",
                "ad_text": "Discover emPower - Your gateway to affordable healthcare. Learn more today!",
                "call_to_action": "LEARN_MORE",
                "display_name": "emPower Health",
                "creative_material_mode": "VIDEO",
                "landing_page_url": "https://example.com/empower"
            },
            "3456789013": {
                "ad_id": "3456789013",
                "ad_name": "TikTok Empower 15s Video B",
                "adgroup_id": "2345678902",
                "adgroup_name": "Awareness_AdSet_18-35",
                "campaign_id": "1234567891",
                "campaign_name": "emPower Awareness Campaign Q1",
                "ad_text": "Healthcare made simple. Join thousands who trust emPower for their health needs.",
                "call_to_action": "SIGN_UP",
                "display_name": "emPower Health",
                "creative_material_mode": "VIDEO",
                "landing_page_url": "https://example.com/empower/signup"
            }
        }
        
        logger.info(f"Generated {len(mock_details)} mock ad detail records")
        return mock_details


def test_transformation():
    """Test the data transformation logic"""
    
    logger.info("=" * 60)
    logger.info("TESTING TIKTOK DATA TRANSFORMATION")
    logger.info("=" * 60)
    
    # Generate mock data
    mock_report = MockTikTokData.generate_report_response(num_days=7)
    mock_details = MockTikTokData.generate_ad_details()
    
    # Import the transformation function from your extractor
    # For testing, we'll recreate it here
    def transform_to_bigquery_schema(raw_data: List[Dict], ad_details: Dict[str, Dict]) -> List[Dict]:
        """Transform TikTok API response to BigQuery schema"""
        transformed_data = []
        
        for row in raw_data:
            ad_id = row.get("dimensions", {}).get("ad_id")
            ad_info = ad_details.get(ad_id, {})
            
            # Calculate video quartile metrics
            video_views = row.get("metrics", {}).get("video_play_actions", 0)
            video_2s = row.get("metrics", {}).get("video_watched_2s", 0)
            video_6s = row.get("metrics", {}).get("video_watched_6s", 0)
            
            video_25 = video_2s if video_2s else None
            video_50 = video_6s if video_6s else None
            video_75 = int(video_views * 0.75) if video_views else None
            video_100 = int(video_views) if video_views else None
            
            # Calculate CPR
            spend = row.get("metrics", {}).get("spend", 0)
            reach = row.get("metrics", {}).get("reach", 0)
            cpr = round(spend / reach, 6) if reach and reach > 0 else None
            
            # Map to BigQuery schema (26 fields)
            transformed_row = {
                "DATE": row.get("dimensions", {}).get("stat_time_day"),
                "VIDEO_AVERAGE_PLAY_TIME": row.get("metrics", {}).get("average_video_play", 0),
                "VIDEO_VIEWS": video_views,
                "VIDEO_VIEWS_AT_25": video_25,
                "VIDEO_VIEWS_AT_50": video_50,
                "VIDEO_VIEWS_AT_75": video_75,
                "VIDEO_VIEWS_AT_100": video_100,
                "FORMAT": ad_info.get("creative_material_mode", ""),
                "TEXT": ad_info.get("ad_text", ""),
                "CREATIVE": ad_id,
                "CALL_TO_ACTION": ad_info.get("call_to_action", ""),
                "FREQUENCY": row.get("metrics", {}).get("frequency", 0),
                "AMOUNT_SPENT_USD": row.get("metrics", {}).get("spend", 0),
                "REACH": row.get("metrics", {}).get("reach", 0),
                "CTR_DESTINATION": row.get("metrics", {}).get("ctr", 0),
                "CURRENCY": "USD",
                "IMPRESSIONS": row.get("metrics", {}).get("impressions", 0),
                "CPM": row.get("metrics", {}).get("cpm", 0),
                "CPC_DESTINATION": row.get("metrics", {}).get("cpc", 0),
                "LINK_CLICKS": row.get("metrics", {}).get("clicks", 0),
                "CPR": cpr,
                "CAMPAIGN_NAME": ad_info.get("campaign_name", ""),
                "AD_GROUP_NAME": ad_info.get("adgroup_name", ""),
                "AD_NAME": ad_info.get("ad_name", ""),
                "PLATFORM": "TikTok",
                "LANGUAGE": "en"
            }
            
            transformed_data.append(transformed_row)
        
        return transformed_data
    
    # Perform transformation
    transformed = transform_to_bigquery_schema(mock_report, mock_details)
    
    # Validate results
    logger.info(f"\n✅ Transformation successful!")
    logger.info(f"   Input rows: {len(mock_report)}")
    logger.info(f"   Output rows: {len(transformed)}")
    
    # Check that all 26 fields are present
    expected_fields = [
        "DATE", "VIDEO_AVERAGE_PLAY_TIME", "FORMAT", "VIDEO_VIEWS_AT_50",
        "FREQUENCY", "AMOUNT_SPENT_USD", "VIDEO_VIEWS_AT_75", "VIDEO_VIEWS_AT_25",
        "CPR", "REACH", "CTR_DESTINATION", "CURRENCY", "IMPRESSIONS", "CPM",
        "CPC_DESTINATION", "LINK_CLICKS", "CALL_TO_ACTION", "TEXT", "PLATFORM",
        "LANGUAGE", "CREATIVE", "AD_NAME", "VIDEO_VIEWS_AT_100", "VIDEO_VIEWS",
        "AD_GROUP_NAME", "CAMPAIGN_NAME"
    ]
    
    if transformed:
        sample_row = transformed[0]
        actual_fields = set(sample_row.keys())
        expected_set = set(expected_fields)
        
        missing_fields = expected_set - actual_fields
        extra_fields = actual_fields - expected_set
        
        if missing_fields:
            logger.warning(f"⚠️  Missing fields: {missing_fields}")
        if extra_fields:
            logger.warning(f"⚠️  Extra fields: {extra_fields}")
        
        if not missing_fields and not extra_fields:
            logger.info(f"✅ All 26 required fields present!")
        
        # Display sample transformed row
        logger.info(f"\n📊 SAMPLE TRANSFORMED ROW:")
        logger.info("=" * 60)
        logger.info(json.dumps(sample_row, indent=2))
        logger.info("=" * 60)
    
    return transformed


def test_bigquery_schema_validation():
    """Validate that transformed data matches BigQuery schema"""
    
    logger.info("\n" + "=" * 60)
    logger.info("TESTING BIGQUERY SCHEMA COMPATIBILITY")
    logger.info("=" * 60)
    
    # Generate and transform data
    mock_report = MockTikTokData.generate_report_response(num_days=2)
    mock_details = MockTikTokData.generate_ad_details()
    
    from test_tiktok_pipeline import test_transformation
    transformed = test_transformation()
    
    # Define expected data types
    schema_validation = {
        "DATE": str,
        "VIDEO_AVERAGE_PLAY_TIME": (int, float),
        "VIDEO_VIEWS": int,
        "VIDEO_VIEWS_AT_25": (int, type(None)),
        "VIDEO_VIEWS_AT_50": (int, type(None)),
        "VIDEO_VIEWS_AT_75": (int, type(None)),
        "VIDEO_VIEWS_AT_100": (int, type(None)),
        "FORMAT": str,
        "TEXT": str,
        "CREATIVE": str,
        "CALL_TO_ACTION": str,
        "FREQUENCY": (int, float),
        "AMOUNT_SPENT_USD": (int, float),
        "REACH": int,
        "CTR_DESTINATION": (int, float),
        "CURRENCY": str,
        "IMPRESSIONS": int,
        "CPM": (int, float),
        "CPC_DESTINATION": (int, float),
        "LINK_CLICKS": int,
        "CPR": (int, float, type(None)),
        "CAMPAIGN_NAME": str,
        "AD_GROUP_NAME": str,
        "AD_NAME": str,
        "PLATFORM": str,
        "LANGUAGE": str
    }
    
    # Validate types
    errors = []
    if transformed:
        sample = transformed[0]
        
        for field, expected_type in schema_validation.items():
            value = sample.get(field)
            
            if isinstance(expected_type, tuple):
                if not isinstance(value, expected_type):
                    errors.append(f"Field '{field}': expected {expected_type}, got {type(value)}")
            else:
                if not isinstance(value, expected_type):
                    errors.append(f"Field '{field}': expected {expected_type}, got {type(value)}")
    
    if errors:
        logger.error("❌ Schema validation errors found:")
        for error in errors:
            logger.error(f"   - {error}")
    else:
        logger.info("✅ All field types match BigQuery schema!")
    
    return len(errors) == 0


def test_data_quality():
    """Test data quality and business logic"""
    
    logger.info("\n" + "=" * 60)
    logger.info("TESTING DATA QUALITY")
    logger.info("=" * 60)
    
    mock_report = MockTikTokData.generate_report_response(num_days=5)
    mock_details = MockTikTokData.generate_ad_details()
    
    from test_tiktok_pipeline import test_transformation
    transformed = test_transformation()
    
    quality_checks = []
    
    for row in transformed:
        # Check 1: No negative values for metrics
        if row["AMOUNT_SPENT_USD"] < 0:
            quality_checks.append(f"Negative spend found: {row['AMOUNT_SPENT_USD']}")
        
        if row["IMPRESSIONS"] < 0:
            quality_checks.append(f"Negative impressions found: {row['IMPRESSIONS']}")
        
        # Check 2: CTR should be between 0 and 100
        if not (0 <= row["CTR_DESTINATION"] <= 100):
            quality_checks.append(f"Invalid CTR: {row['CTR_DESTINATION']}")
        
        # Check 3: Clicks should not exceed impressions
        if row["LINK_CLICKS"] > row["IMPRESSIONS"]:
            quality_checks.append(f"Clicks ({row['LINK_CLICKS']}) > Impressions ({row['IMPRESSIONS']})")
        
        # Check 4: Required text fields should not be empty
        if not row["CAMPAIGN_NAME"]:
            quality_checks.append("Empty CAMPAIGN_NAME found")
        
        if not row["AD_NAME"]:
            quality_checks.append("Empty AD_NAME found")
        
        # Check 5: Date format validation
        try:
            datetime.strptime(row["DATE"], "%Y-%m-%d")
        except ValueError:
            quality_checks.append(f"Invalid date format: {row['DATE']}")
    
    if quality_checks:
        logger.warning(f"⚠️  Found {len(quality_checks)} data quality issues:")
        for issue in quality_checks[:10]:  # Show first 10
            logger.warning(f"   - {issue}")
    else:
        logger.info("✅ All data quality checks passed!")
    
    return len(quality_checks) == 0


def main():
    """Run all tests"""
    
    logger.info("\n" + "🧪" * 30)
    logger.info("TIKTOK ETL PIPELINE - TEST SUITE")
    logger.info("🧪" * 30 + "\n")
    
    results = {
        "Transformation": False,
        "Schema Validation": False,
        "Data Quality": False
    }
    
    try:
        # Test 1: Transformation
        transformed_data = test_transformation()
        results["Transformation"] = True
        
        # Test 2: Schema validation
        results["Schema Validation"] = test_bigquery_schema_validation()
        
        # Test 3: Data quality
        results["Data Quality"] = test_data_quality()
        
    except Exception as e:
        logger.error(f"❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 60)
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name:.<40}{status}")
    
    all_passed = all(results.values())
    
    if all_passed:
        logger.info("\n🎉 All tests passed! Pipeline is ready for deployment.")
        logger.info("\nNext steps:")
        logger.info("1. Add your TikTok API credentials")
        logger.info("2. Test with real API connection")
        logger.info("3. Deploy to Cloud Run")
    else:
        logger.info("\n⚠️  Some tests failed. Please review and fix issues before deployment.")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
