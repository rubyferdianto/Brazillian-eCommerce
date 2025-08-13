#!/bin/bash
# Dataflow Job Execution Script
# MongoDB to Google Cloud Storage Export Pipeline

# =============================================================================
# LOAD ENVIRONMENT VARIABLES
# =============================================================================

# Load environment variables from .env file
if [ -f .env ]; then
    echo "📄 Loading environment variables from .env file..."
    export $(cat .env | xargs)
else
    echo "⚠️  .env file not found. Please create one from .env.example"
    echo "💡 cp .env.example .env"
    echo "💡 Then edit .env with your actual credentials"
    exit 1
fi

# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================

# Validate required environment variables
if [ -z "$MONGO_URI" ]; then
    echo "❌ MONGO_URI not set in .env file"
    exit 1
fi

if [ -z "$GCP_PROJECT_ID" ]; then
    echo "❌ GCP_PROJECT_ID not set in .env file"
    exit 1
fi

if [ -z "$GCS_BUCKET" ]; then
    echo "❌ GCS_BUCKET not set in .env file"
    exit 1
fi

# Set defaults from environment variables
export PROJECT_ID="${GCP_PROJECT_ID}"
export REGION="${GCP_REGION:-us-central1}"
export MONGO_DATABASE="${MONGO_DATABASE:-brazilian-ecommerce}"
export MACHINE_TYPE="${DATAFLOW_MACHINE_TYPE:-n1-standard-2}"
export MAX_WORKERS="${DATAFLOW_MAX_WORKERS:-10}"

# Generate unique job name
export JOB_NAME="mongodb-to-gcs-$(date +%Y%m%d-%H%M%S)"

echo "🚀 MongoDB to GCS Dataflow Pipeline"
echo "=================================="

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo "❌ Google Cloud SDK not found. Please install it first:"
    echo "   curl https://sdk.cloud.google.com | bash"
    echo "   exec -l \$SHELL"
    echo "   gcloud init"
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "❌ Not authenticated with Google Cloud. Please run:"
    echo "   gcloud auth login"
    echo "   gcloud auth application-default login"
    exit 1
fi

# Set project
echo "📝 Setting Google Cloud project: $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔧 Enabling required Google Cloud APIs..."
gcloud services enable dataflow.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com

# Create GCS bucket if it doesn't exist
echo "📦 Creating GCS bucket: $GCS_BUCKET"
gsutil mb -p $PROJECT_ID -l $REGION gs://$GCS_BUCKET 2>/dev/null || echo "✅ Bucket already exists"

# Create required directories in bucket
echo "📁 Creating bucket directories..."
gsutil -m cp /dev/null gs://$GCS_BUCKET/temp/.keep 2>/dev/null || true
gsutil -m cp /dev/null gs://$GCS_BUCKET/staging/.keep 2>/dev/null || true
gsutil -m cp /dev/null gs://$GCS_BUCKET/brazilian-ecommerce-exports/.keep 2>/dev/null || true

# =============================================================================
# PIPELINE EXECUTION
# =============================================================================

echo "🔄 Submitting Dataflow job: $JOB_NAME"
echo "📊 Collections to export: customers, orders, order_items, order_payments, order_reviews, products, sellers, geolocation, product_categories"

# Run the Dataflow pipeline
python mongodb_to_gcs_pipeline.py \
    --project=$PROJECT_ID \
    --gcs_bucket=$GCS_BUCKET \
    --mongo_uri="$MONGO_URI" \
    --mongo_database=$MONGO_DATABASE \
    --temp_location=gs://$GCS_BUCKET/temp \
    --staging_location=gs://$GCS_BUCKET/staging \
    --runner=DataflowRunner \
    --region=$REGION \
    --job_name=$JOB_NAME \
    --machine_type=$MACHINE_TYPE \
    --max_num_workers=$MAX_WORKERS \
    --setup_file=./setup.py \
    --collections=customers,orders,order_items,order_payments,order_reviews,products,sellers,geolocation,product_categories

# Check if job submission was successful
if [ $? -eq 0 ]; then
    echo "✅ Dataflow job submitted successfully!"
    echo "📊 Job Name: $JOB_NAME"
    echo "🌐 Monitor progress at: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_NAME?project=$PROJECT_ID"
    echo "📂 Output will be available at: gs://$GCS_BUCKET/brazilian-ecommerce-exports/"
    
    echo ""
    echo "💡 Useful commands:"
    echo "   # Check job status"
    echo "   gcloud dataflow jobs describe $JOB_NAME --region=$REGION"
    echo ""
    echo "   # List output files"
    echo "   gsutil ls gs://$GCS_BUCKET/brazilian-ecommerce-exports/"
    echo ""
    echo "   # Download a file"
    echo "   gsutil cp gs://$GCS_BUCKET/brazilian-ecommerce-exports/customers-00000-of-00001.csv ."
else
    echo "❌ Failed to submit Dataflow job"
    exit 1
fi
