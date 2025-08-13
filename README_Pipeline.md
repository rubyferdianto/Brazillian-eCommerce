# MongoDB to Google Cloud Storage - Apache Beam Pipeline

## 🎯 Project Overview

This project provides a complete Apache Beam pipeline solution for extracting data from MongoDB collections and exporting them as CSV files to Google Cloud Storage using Google Cloud Dataflow.

## 📁 Files Created

### Core Pipeline Files
- **`mongodb_to_gcs_pipeline.py`** - Main Apache Beam pipeline code
- **`requirements.txt`** - Python dependencies (MongoDB + Apache Beam + GCP)
- **`setup.py`** - Setup configuration for Dataflow workers

### Execution Scripts
- **`run_dataflow_job.sh`** - Automated Dataflow job execution script ✨
- **`test_pipeline.py`** - Local testing and validation script

### Documentation
- **`Pipeline_Guide.md`** - Comprehensive setup and usage guide
- **`README_Pipeline.md`** - This summary file

## 🚀 Quick Start

### 1. Prerequisites Setup
```bash
# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate with Google Cloud
gcloud auth login
gcloud auth application-default login

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Test Your Setup
```bash
# Run local tests first
python test_pipeline.py

# Quick connection test
python test_pipeline.py --quick
```

### 3. Configure and Run
```bash
# Edit the configuration in the script
nano run_dataflow_job.sh

# Update these variables:
# - PROJECT_ID="your-gcp-project-id"
# - GCS_BUCKET="your-gcs-bucket-name"

# Execute the pipeline
./run_dataflow_job.sh
```

## 📊 Data Pipeline Flow

```
MongoDB Atlas (Brazilian E-commerce)
        ↓
   [Extract Phase]
MongoDB Collections:
- customers (99,441 records)
- orders (99,441 records)  
- order_items (112,650 records)
- order_payments (103,886 records)
- order_reviews (99,224 records)
- products (32,951 records)
- sellers (3,095 records)
- geolocation (1,000,163 records)
- product_categories (71 records)
        ↓
   [Transform Phase]
- Flatten nested JSON documents
- Convert to CSV format
- Handle data type conversions
        ↓
   [Load Phase]
Google Cloud Storage
- customers.csv
- orders.csv
- order_items.csv
- order_payments.csv
- order_reviews.csv
- products.csv
- sellers.csv
- geolocation.csv
- product_categories.csv
```

## 💰 Cost Estimation

**Estimated Dataflow Job Cost**: $15-30 USD
- **Data Volume**: ~1.8M total records
- **Runtime**: 15-30 minutes
- **Machine Type**: n1-standard-2
- **Max Workers**: 10

*Costs may vary based on region and actual runtime*

## 🔧 Configuration Options

### Key Parameters You Can Modify:

```bash
# In run_dataflow_job.sh:
PROJECT_ID="your-project"           # Your GCP project
GCS_BUCKET="your-bucket"           # Output bucket name
REGION="us-central1"               # GCP region
MACHINE_TYPE="n1-standard-2"       # Worker machine type
MAX_WORKERS="10"                   # Maximum workers
```

### Pipeline Collections:
By default exports all 9 collections. Customize in the script:
```bash
--collections=customers,orders,order_items  # Export specific collections only
```

## 📈 Monitoring Your Job

### During Execution:
1. **Console Monitoring**: Visit [Google Cloud Dataflow Console](https://console.cloud.google.com/dataflow)
2. **Command Line**: `gcloud dataflow jobs list --region=us-central1`
3. **Job Logs**: Available in Cloud Logging

### After Completion:
```bash
# Check output files
gsutil ls gs://YOUR_BUCKET/brazilian-ecommerce-exports/

# Download files
gsutil -m cp gs://YOUR_BUCKET/brazilian-ecommerce-exports/*.csv ./downloads/
```

## 🛠️ Troubleshooting

### Common Issues:

**1. Authentication Errors**
```bash
gcloud auth application-default login
```

**2. Missing APIs**
```bash
gcloud services enable dataflow.googleapis.com storage.googleapis.com
```

**3. MongoDB Connection Issues**
- Verify Atlas IP whitelist includes Google Cloud IPs
- Check connection string in the script

**4. GCS Permissions**
```bash
gsutil iam ch user:YOUR_EMAIL:objectAdmin gs://YOUR_BUCKET
```

## 🎯 Success Indicators

✅ **Pipeline Submission**: Job appears in Dataflow console  
✅ **Worker Startup**: Workers launch and connect to MongoDB  
✅ **Data Processing**: Progress shown in pipeline graph  
✅ **File Creation**: CSV files appear in GCS bucket  
✅ **Job Completion**: Status shows "Succeeded"

## 📋 Next Steps After Export

1. **Data Analysis**: Import CSV files into BigQuery for SQL analysis
2. **Visualization**: Use Looker Studio or Data Studio for dashboards
3. **Machine Learning**: Use the data with Vertex AI or AutoML
4. **Scheduling**: Set up Cloud Scheduler for regular exports

## 🔄 Automation Options

### For Regular Exports:
```bash
# Create a Cloud Function triggered by Cloud Scheduler
# Use the same pipeline code with different triggers
```

### For Real-time Processing:
```bash
# Consider using Change Streams from MongoDB
# With Pub/Sub for real-time data pipeline
```

## 📞 Support Resources

- **Apache Beam**: [Documentation](https://beam.apache.org/documentation/)
- **Cloud Dataflow**: [Troubleshooting Guide](https://cloud.google.com/dataflow/docs/guides/troubleshooting-your-pipeline)
- **MongoDB**: [Connection Issues](https://docs.mongodb.com/manual/reference/connection-string/)

---

**Ready to Export Your Data? Run `./run_dataflow_job.sh` to get started!** 🚀
