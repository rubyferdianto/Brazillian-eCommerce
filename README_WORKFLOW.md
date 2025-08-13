# Google Cloud Workflow Integration for MongoDB to GCS Export

This solution integrates our MongoDB CSV export functionality with Google Cloud Workflows for automated, scalable data processing.

## 🏗️ Architecture Overview

```
MongoDB Atlas → Cloud Function → Google Cloud Storage
     ↑              ↑                    ↑
     |              |                    |
Environment     Workflow            Organized
Variables      Orchestration         CSV Files
```

## 📁 Project Structure

```
├── workflow.yaml              # Google Cloud Workflow definition
├── cloud_function/
│   ├── main.py               # Cloud Function code
│   └── requirements.txt      # Python dependencies
├── deploy.sh                 # Deployment automation script
├── test_local.sh            # Local testing script  
├── test_payload.json        # Sample request payload
└── README_WORKFLOW.md       # This documentation
```

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate
gcloud auth login
gcloud auth application-default login

# Set your project
export GOOGLE_CLOUD_PROJECT="your-project-id"
export REGION="us-central1"
export BUCKET_NAME="brazilian-ecommerce-data"
```

### 2. Deploy the Solution

```bash
# Make deployment script executable
chmod +x deploy.sh

# Deploy everything to Google Cloud
./deploy.sh
```

### 3. Set Environment Variables

After deployment, set the MongoDB connection string in your Cloud Function:

```bash
gcloud functions deploy mongodb-csv-exporter \
    --update-env-vars="MONGO_URI=your_mongodb_connection_string"
```

### 4. Execute the Workflow

```bash
# Execute the workflow
./execute_workflow.sh your-bucket-name
```

## 🧪 Local Testing

Test the Cloud Function locally before deploying:

```bash
# Start local testing server
chmod +x test_local.sh
./test_local.sh
```

Then in another terminal:

```bash
# Test the function
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

## 📊 Workflow Features

### Input Parameters
- `bucket_name`: Target GCS bucket (default: "brazilian-ecommerce-data")
- `collections`: List of collections to export (optional)
- `export_folder`: Folder name in bucket (auto-generated with timestamp)

### Output Structure
```
gs://your-bucket/exports_20250813_120000/
├── customers.csv
├── sellers.csv
├── orders.csv
├── order_items.csv
├── order_payments.csv
├── order_reviews.csv
├── products.csv
├── geolocation.csv
└── product_categories.csv
```

### Error Handling
- MongoDB connection failures
- Individual collection export errors
- GCS upload failures
- Comprehensive logging and monitoring

## ⚙️ Configuration Options

### Workflow Customization

Edit `workflow.yaml` to:
- Change default bucket names
- Modify retry policies
- Add notification steps
- Integrate with other services

### Function Scaling

The Cloud Function is configured with:
- **Memory**: 2GB (handles large datasets)
- **Timeout**: 9 minutes (540s)
- **Max Instances**: 10 (parallel processing)
- **Runtime**: Python 3.11

### Performance Tuning

For large datasets:
```bash
# Increase memory and timeout
gcloud functions deploy mongodb-csv-exporter \
    --memory=4GB \
    --timeout=900s \
    --max-instances=20
```

## 📈 Monitoring & Observability

### Cloud Console Links
- **Workflows**: https://console.cloud.google.com/workflows
- **Functions**: https://console.cloud.google.com/functions
- **Storage**: https://console.cloud.google.com/storage
- **Logs**: https://console.cloud.google.com/logs

### Key Metrics to Monitor
- Workflow execution success rate
- Function execution time
- Memory usage
- Data transfer volumes
- Error rates

## 🔧 Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**
   ```bash
   # Check environment variables
   gcloud functions describe mongodb-csv-exporter --format="value(serviceConfig.environmentVariables)"
   
   # Update MONGO_URI
   gcloud functions deploy mongodb-csv-exporter --update-env-vars="MONGO_URI=new_connection_string"
   ```

2. **GCS Permission Errors**
   ```bash
   # Grant Storage Admin role to function service account
   gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
       --member="serviceAccount:$GOOGLE_CLOUD_PROJECT@appspot.gserviceaccount.com" \
       --role="roles/storage.admin"
   ```

3. **Function Timeout**
   ```bash
   # Increase timeout (max 540s for gen2)
   gcloud functions deploy mongodb-csv-exporter --timeout=540s
   ```

### Debugging Commands

```bash
# View function logs
gcloud functions logs read mongodb-csv-exporter --limit=50

# Check workflow execution
gcloud workflows executions list --workflow=mongodb-to-gcs-workflow --location=$REGION

# Test function directly
curl -X POST $FUNCTION_URL -H "Content-Type: application/json" -d @test_payload.json
```

## 🔄 CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy MongoDB Export Workflow
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: google-github-actions/setup-gcloud@v0
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT }}
      - run: ./deploy.sh
```

## 🎯 Use Cases

### Scheduled Exports
```bash
# Create a Cloud Scheduler job for daily exports
gcloud scheduler jobs create http mongodb-daily-export \
    --schedule="0 2 * * *" \
    --uri="$WORKFLOW_TRIGGER_URL" \
    --http-method=POST \
    --message-body='{"bucket_name":"daily-exports"}'
```

### Event-Driven Processing
- Trigger on database changes
- Process new data automatically
- Integration with Apache Beam pipeline

### Data Pipeline Integration
- Export → Transform → Load (ETL)
- Integration with BigQuery
- Machine learning preprocessing

## 📋 Cost Optimization

### Resource Management
- Use appropriate function memory settings
- Monitor and optimize batch sizes
- Implement intelligent retry policies
- Use regional resources to reduce latency

### Estimated Costs (Monthly)
- **Cloud Function**: $5-20 (based on executions)
- **Workflow**: $1-5 (based on executions)  
- **Storage**: $0.02 per GB stored
- **Network**: $0.12 per GB transferred

## 🔐 Security Best Practices

### Environment Variables
- Store sensitive data in Secret Manager
- Use IAM for access control
- Rotate credentials regularly
- Monitor access logs

### Network Security
- Use VPC connectors for private networks
- Implement IP restrictions
- Enable audit logging
- Use service account keys securely

## 📚 Additional Resources

- [Google Cloud Workflows Documentation](https://cloud.google.com/workflows/docs)
- [Cloud Functions Documentation](https://cloud.google.com/functions/docs)
- [MongoDB Atlas Documentation](https://docs.atlas.mongodb.com/)
- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)

---

**Next Steps**: After successful deployment, consider integrating with:
- Apache Beam for advanced data processing
- BigQuery for analytics
- Cloud Scheduler for automation
- Monitoring and alerting systems
