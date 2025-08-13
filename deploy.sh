#!/bin/bash

# Google Cloud Deployment Script for MongoDB to GCS Workflow
# This script deploys the Cloud Function and Workflow

set -e

# Configuration
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-your-project-id}"
REGION="${REGION:-us-central1}"
FUNCTION_NAME="mongodb-csv-exporter"
WORKFLOW_NAME="mongodb-to-gcs-workflow"
BUCKET_NAME="${BUCKET_NAME:-brazilian-ecommerce-data}"

echo "🚀 Deploying MongoDB to GCS Export Solution"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "="

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI not found. Please install Google Cloud SDK."
    exit 1
fi

# Set the project
echo "📋 Setting Google Cloud project..."
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔧 Enabling required Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable workflows.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable cloudbuild.googleapis.com

# Create GCS bucket if it doesn't exist
echo "📂 Creating GCS bucket: $BUCKET_NAME"
gsutil ls gs://$BUCKET_NAME 2>/dev/null || gsutil mb gs://$BUCKET_NAME
echo "✅ Bucket ready: gs://$BUCKET_NAME"

# Deploy Cloud Function
echo "☁️ Deploying Cloud Function: $FUNCTION_NAME"
cd cloud_function
gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=mongodb_csv_exporter \
    --trigger=http \
    --allow-unauthenticated \
    --memory=2GB \
    --timeout=540s \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID" \
    --max-instances=10

echo "✅ Cloud Function deployed successfully!"

# Get the function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(serviceConfig.uri)")
echo "📡 Function URL: $FUNCTION_URL"

cd ..

# Update workflow with function URL
echo "📝 Updating workflow configuration..."
sed -i.bak "s|https://.*cloudfunctions.net/mongodb-csv-exporter|$FUNCTION_URL|g" workflow.yaml

# Deploy Workflow
echo "⚡ Deploying Google Cloud Workflow: $WORKFLOW_NAME"
gcloud workflows deploy $WORKFLOW_NAME \
    --source=workflow.yaml \
    --location=$REGION

echo "✅ Workflow deployed successfully!"

# Create a sample execution script
cat > execute_workflow.sh << EOF
#!/bin/bash

# Execute the MongoDB to GCS export workflow
# Usage: ./execute_workflow.sh [bucket_name]

BUCKET_NAME=\${1:-$BUCKET_NAME}

echo "🚀 Executing MongoDB to GCS export workflow..."
echo "Bucket: \$BUCKET_NAME"

gcloud workflows run $WORKFLOW_NAME \\
    --location=$REGION \\
    --data="{\\\"bucket_name\\\": \\\"\$BUCKET_NAME\\\"}"

echo "✅ Workflow execution started!"
echo "💡 Monitor progress at: https://console.cloud.google.com/workflows"
EOF

chmod +x execute_workflow.sh

echo ""
echo "🎉 Deployment Complete!"
echo "="
echo "📂 GCS Bucket: gs://$BUCKET_NAME"
echo "☁️ Function: $FUNCTION_URL"
echo "⚡ Workflow: $WORKFLOW_NAME"
echo ""
echo "🚀 To execute the workflow:"
echo "   ./execute_workflow.sh"
echo ""
echo "📊 Monitor at:"
echo "   https://console.cloud.google.com/workflows/workflow/$REGION/$WORKFLOW_NAME"
echo ""
echo "🔧 Remember to set these environment variables in your Cloud Function:"
echo "   MONGO_URI=your_mongodb_connection_string"
