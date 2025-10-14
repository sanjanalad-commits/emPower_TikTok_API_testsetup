# TikTok ETL Pipeline - Deployment Guide

Automated TikTok Marketing API data extraction to Google BigQuery, deployed on Cloud Run.

## 📋 Overview

This pipeline automatically extracts ad performance data from TikTok Marketing API and loads it into BigQuery table `slstrategy.EMPOWER_2025.TIKTOKREPORT_RAW`.

**Features:**
- ✅ Extracts all 26 required fields from TikTok API
- ✅ Handles pagination automatically
- ✅ Transforms data to match BigQuery schema
- ✅ Deployed as serverless container on Cloud Run
- ✅ Scheduled execution via Cloud Scheduler
- ✅ Secure credential storage in Secret Manager

---

## 🚀 Quick Start

### Prerequisites

1. **Google Cloud Project** with billing enabled
2. **TikTok Developer Account** with Marketing API approval
3. **API Credentials** from TikTok Developer Portal:
   - App ID
   - App Secret
   - Access Token
   - Advertiser ID

### Required Google Cloud Services

Enable these APIs in your GCP project:

```bash
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable bigquery.googleapis.com
```

---

## 📦 File Structure

```
tiktok-etl-pipeline/
├── main.py                      # FastAPI application
├── tiktok_extractor.py          # TikTok API extractor logic
├── test_tiktok_pipeline.py      # Test suite with mock data
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Container configuration
├── README.md                    # This file
└── .gitignore                   # Git ignore rules
```

---

## 🔧 Setup Instructions

### Step 1: Clone or Create Repository

```bash
# In Google Cloud Shell
mkdir tiktok-etl-pipeline
cd tiktok-etl-pipeline

# Copy all the files provided:
# - main.py
# - tiktok_extractor.py
# - test_tiktok_pipeline.py
# - requirements.txt
# - Dockerfile
```

### Step 2: Test Locally (Without TikTok Credentials)

Test the transformation logic with mock data:

```bash
# Install dependencies
pip install -r requirements.txt

# Run test suite
python test_tiktok_pipeline.py
```

Expected output:
```
✅ Transformation successful!
✅ All 26 required fields present!
✅ All field types match BigQuery schema!
✅ All data quality checks passed!
🎉 All tests passed! Pipeline is ready for deployment.
```

### Step 3: Store TikTok Credentials in Secret Manager

Once you have your TikTok API credentials:

```bash
# Set your project ID
export PROJECT_ID="your-gcp-project-id"

# Create secret with TikTok credentials
echo '{
  "app_id": "YOUR_APP_ID",
  "app_secret": "YOUR_APP_SECRET",
  "access_token": "YOUR_ACCESS_TOKEN",
  "advertiser_id": "YOUR_ADVERTISER_ID"
}' | gcloud secrets create tiktok-api-credentials \
  --data-file=- \
  --project=$PROJECT_ID
```

### Step 4: Update Configuration

Edit `main.py` and set your project configuration:

```python
# In load_credentials() function, update:
project_id = os.getenv("GCP_PROJECT_ID", "YOUR_PROJECT_ID")

# Verify BigQuery settings match your setup:
dataset_id = "slstrategy.EMPOWER_2025"
table_id = "TIKTOKREPORT_RAW"
```

### Step 5: Build and Deploy to Cloud Run

```bash
# Build container image
gcloud builds submit --tag gcr.io/$PROJECT_ID/tiktok-etl-pipeline

# Deploy to Cloud Run
gcloud run deploy tiktok-etl-pipeline \
  --image gcr.io/$PROJECT_ID/tiktok-etl-pipeline \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --timeout 3600 \
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID" \
  --set-env-vars "USE_SECRET_MANAGER=true" \
  --set-env-vars "BIGQUERY_DATASET=slstrategy.EMPOWER_2025" \
  --set-env-vars "BIGQUERY_TABLE=TIKTOKREPORT_RAW" \
  --allow-unauthenticated \
  --project=$PROJECT_ID
```

**Note:** The deploy command will output a service URL. Save this for the next step.

### Step 6: Set Up Cloud Scheduler

Schedule daily automatic runs:

```bash
# Get your Cloud Run service URL
export SERVICE_URL=$(gcloud run services describe tiktok-etl-pipeline \
  --platform managed \
  --region us-central1 \
  --format 'value(status.url)')

# Create daily schedule (runs at 6 AM UTC)
gcloud scheduler jobs create http daily-tiktok-etl \
  --schedule="0 6 * * *" \
  --uri="$SERVICE_URL/extract" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"write_mode":"WRITE_APPEND"}' \
  --location=us-central1 \
  --project=$PROJECT_ID
```

---

## 🧪 Testing the Pipeline

### Test the API Endpoints

```bash
# Get your service URL
SERVICE_URL=$(gcloud run services describe tiktok-etl-pipeline \
  --platform managed \
  --region us-central1 \
  --format 'value(status.url)')

# Health check
curl $SERVICE_URL/health

# Check status
curl $SERVICE_URL/status

# Manual trigger (last 7 days)
curl -X POST $SERVICE_URL/extract \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2025-10-07",
    "end_date": "2025-10-13",
    "write_mode": "WRITE_APPEND"
  }'
```

### Backfill Historical Data

To load historical data when first setting up:

```bash
# Backfill last 6 months
curl -X POST "$SERVICE_URL/backfill?months=6&write_mode=WRITE_APPEND"
```

---

## 📊 Monitoring

### View Logs

```bash
# View recent logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=tiktok-etl-pipeline" \
  --limit 50 \
  --format json

# Follow logs in real-time
gcloud logging tail "resource.type=cloud_run_revision AND resource.labels.service_name=tiktok-etl-pipeline"
```

### Check BigQuery Data

```sql
-- Check row count
SELECT COUNT(*) as total_rows
FROM `slstrategy.EMPOWER_2025.TIKTOKREPORT_RAW`;

-- View recent data
SELECT *
FROM `slstrategy.EMPOWER_2025.TIKTOKREPORT_RAW`
ORDER BY DATE DESC
LIMIT 100;

-- Check date range
SELECT 
  MIN(DATE) as earliest_date,
  MAX(DATE) as latest_date,
  COUNT(DISTINCT DATE) as unique_dates
FROM `slstrategy.EMPOWER_2025.TIKTOKREPORT_RAW`;
```

---

## 🔄 Update and Redeploy

When you need to update the code:

```bash
# Rebuild and redeploy
gcloud builds submit --tag gcr.io/$PROJECT_ID/tiktok-etl-pipeline
gcloud run deploy tiktok-etl-pipeline \
  --image gcr.io/$PROJECT_ID/tiktok-etl-pipeline \
  --platform managed \
  --region us-central1
```

---

## 🐛 Troubleshooting

### Common Issues

**Issue: "Failed to retrieve secret"**
- Check Secret Manager permissions for Cloud Run service account
- Grant `Secret Manager Secret Accessor` role:
  ```bash
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
  ```

**Issue: "TikTok API returned error"**
- Verify your access token is still valid (TikTok tokens expire)
- Check that your app has Marketing API permissions approved
- Ensure Advertiser ID is correct

**Issue: "No data returned from TikTok API"**
- Check date range - TikTok may not have data for recent dates (usually 2-3 day delay)
- Verify ads were running during the specified date range
- Check TikTok API rate limits

**Issue: "BigQuery load failed"**
- Verify table schema matches the 26 expected columns
- Check BigQuery permissions for service account
- Review error logs for specific schema mismatches

---

## 🔐 Security Best Practices

1. **Never commit credentials** to version control
2. **Use Secret Manager** for all sensitive data
3. **Enable VPC Service Controls** for production
4. **Restrict Cloud Run ingress** to internal only if possible
5. **Monitor API usage** to detect anomalies
6. **Rotate access tokens** regularly

---

## 📈 Next Steps

Once TikTok is working:

1. ✅ **Add Meta API** extractor (when login issues resolved)
2. ✅ **Add Google Ads API** extractor (when review complete)
3. ✅ **Set up monitoring alerts** for pipeline failures
4. ✅ **Create data quality dashboards** in Looker Studio
5. ✅ **Optimize Cloud Scheduler** timing based on data freshness

---

## 📞 Support

For issues or questions:
- Check Cloud Logging for detailed error messages
- Review TikTok API documentation: https://business-api.tiktok.com/portal/docs
- Verify BigQuery table schema matches expected fields

---

## 📝 Notes

- **Data Freshness:** TikTok data typically has a 2-3 day delay
- **Video Metrics:** Quartile views are estimated from available metrics
- **Rate Limits:** TikTok has API rate limits - the pipeline handles pagination automatically
- **Costs:** Cloud Run is serverless - you only pay when the pipeline runs

---

## ✅ Checklist

Before going live:

- [ ] TikTok Developer account approved
- [ ] API credentials stored in Secret Manager
- [ ] Local tests pass with mock data
- [ ] Container deployed to Cloud Run
- [ ] Cloud Scheduler job created
- [ ] Test extraction successful
- [ ] BigQuery data verified
- [ ] Monitoring and alerts configured
- [ ] Documentation updated for your team
