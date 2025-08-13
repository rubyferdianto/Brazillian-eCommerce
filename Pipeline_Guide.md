# MongoDB to Google Cloud Storage Pipeline

This pipeline uses Apache Beam and Google Cloud Dataflow to extract data from MongoDB collections and export them as CSV files to Google Cloud Storage.

## 🎯 Overview

The pipeline performs the following operations:
1. **Extract**: Reads data from MongoDB collections in your Brazilian e-commerce database
2. **Transform**: Converts MongoDB documents to CSV format with proper handling of nested data
3. **Load**: Uploads CSV files to Google Cloud Storage bucket

## 📋 Prerequisites

### 1. Google Cloud Setup
```bash
# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate
gcloud auth login
gcloud auth application-default login

# Set your project
gcloud config set project YOUR_PROJECT_ID
```

### 2. Python Environment
```bash
# Create virtual environment
python -m venv beam-env
source beam-env/bin/activate  # On macOS/Linux
# beam-env\Scripts\activate  # On Windows

# Install dependencies
pip install -r beam_requirements.txt
```

### 3. MongoDB Data
Ensure your MongoDB database contains the Brazilian e-commerce data using the import scripts provided earlier.

## 🚀 Quick Start

### Option 1: Use the Automated Script (Recommended)
```bash
# Make script executable
chmod +x run_dataflow_job.sh

# Edit the script to set your project ID and bucket name
nano run_dataflow_job.sh

# Run the pipeline
./run_dataflow_job.sh
```

### Option 2: Manual Execution
```bash
# Set environment variables
export PROJECT_ID="your-gcp-project-id"
export GCS_BUCKET="your-gcs-bucket-name"

# Run the pipeline
python mongodb_to_gcs_pipeline.py \
    --project=$PROJECT_ID \
    --gcs_bucket=$GCS_BUCKET \
    --temp_location=gs://$GCS_BUCKET/temp \
    --staging_location=gs://$GCS_BUCKET/staging \
    --runner=DataflowRunner \
    --region=us-central1
```

## 🔧 Configuration Options

### Pipeline Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--project` | Required | Google Cloud Project ID |
| `--gcs_bucket` | Required | GCS bucket name (without gs:// prefix) |
| `--mongo_uri` | Atlas URI | MongoDB connection string |
| `--mongo_database` | brazilian-ecommerce | MongoDB database name |
| `--collections` | All collections | Comma-separated list of collections to export |
| `--runner` | DataflowRunner | Pipeline runner (DirectRunner for local testing) |
| `--region` | us-central1 | Google Cloud region |
| `--machine_type` | n1-standard-1 | Dataflow worker machine type |
| `--max_num_workers` | 5 | Maximum number of workers |

### MongoDB Collections Exported

The pipeline exports these collections by default:
- `customers` → `customers.csv`
- `orders` → `orders.csv`
- `order_items` → `order_items.csv`
- `order_payments` → `order_payments.csv`
- `order_reviews` → `order_reviews.csv`
- `products` → `products.csv`
- `sellers` → `sellers.csv`
- `geolocation` → `geolocation.csv`
- `product_categories` → `product_categories.csv`

## 🧪 Local Testing

Test the pipeline locally before running on Dataflow:

```bash
# Test with DirectRunner (local execution)
python mongodb_to_gcs_pipeline.py \
    --project=test-project \
    --gcs_bucket=test-bucket \
    --runner=DirectRunner \
    --collections=customers \
    --temp_location=./temp \
    --staging_location=./staging
```

## 📊 Monitoring and Troubleshooting

### Monitor Dataflow Jobs
```bash
# List running jobs
gcloud dataflow jobs list --region=us-central1

# Get job details
gcloud dataflow jobs describe JOB_ID --region=us-central1

# View logs
gcloud dataflow jobs show JOB_ID --region=us-central1
```

### Check Output Files
```bash
# List exported files
gsutil ls gs://YOUR_BUCKET/brazilian-ecommerce-exports/

# Download a file
gsutil cp gs://YOUR_BUCKET/brazilian-ecommerce-exports/customers-00000-of-00001.csv ./
```

### Common Issues

**1. Authentication Errors**
```bash
# Re-authenticate
gcloud auth application-default login
```

**2. MongoDB Connection Issues**
- Verify MongoDB Atlas IP whitelist includes Google Cloud IPs
- Check connection string credentials

**3. GCS Permission Issues**
```bash
# Grant storage permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="user:YOUR_EMAIL" \
    --role="roles/storage.admin"
```

**4. Dataflow API Not Enabled**
```bash
# Enable required APIs
gcloud services enable dataflow.googleapis.com
gcloud services enable storage.googleapis.com
```

## 💰 Cost Optimization

### Dataflow Pricing Tips
- Use `n1-standard-1` for small datasets
- Set appropriate `max_num_workers` based on data size
- Use `preemptible` workers for cost savings:
  ```bash
  --use_public_ips=false \
  --worker_zone=us-central1-a \
  --preemptible_worker_zone=us-central1-a
  ```

### Estimated Costs (US Central1)
- Small dataset (< 1M records): $5-10
- Medium dataset (1-10M records): $10-25
- Large dataset (> 10M records): $25-50

## 🔄 Pipeline Architecture

```
MongoDB Collections
        ↓
[MongoDBReadTransform]
        ↓
[ConvertToCSVTransform]
        ↓
[WriteToText (GCS)]
        ↓
Google Cloud Storage
```

## 📝 File Structure

```
├── mongodb_to_gcs_pipeline.py    # Main pipeline code
├── beam_requirements.txt         # Python dependencies
├── setup.py                     # Setup for Dataflow workers
├── run_dataflow_job.sh          # Automated execution script
└── Pipeline_Guide.md            # This documentation
```

## 🤝 Support

For issues or questions:
1. Check the [Apache Beam documentation](https://beam.apache.org/documentation/)
2. Review [Google Cloud Dataflow troubleshooting](https://cloud.google.com/dataflow/docs/guides/troubleshooting-your-pipeline)
3. Check MongoDB connection and permissions

## 📈 Performance Tuning

### For Large Datasets
```bash
# Increase worker resources
--machine_type=n1-standard-4 \
--max_num_workers=20 \
--disk_size_gb=100
```

### For Many Small Collections
```bash
# Reduce workers for efficiency
--machine_type=n1-standard-1 \
--max_num_workers=3
```

## 🔐 Security Best Practices

1. **Use service accounts** instead of user credentials for production
2. **Rotate MongoDB credentials** regularly
3. **Use VPC networks** for enhanced security
4. **Enable audit logging** for compliance
5. **Encrypt data at rest** in GCS buckets

---

**Next Steps**: After successful export, you can use the CSV files in Google Cloud for analytics with BigQuery, Data Studio, or other tools.
