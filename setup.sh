#!/bin/bash
# Quick Setup Script for Brazilian E-commerce Pipeline

echo "🚀 Brazilian E-commerce Data Pipeline Setup"
echo "=========================================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "📄 Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created!"
    echo ""
    echo "⚠️  IMPORTANT: Please edit .env file with your actual credentials:"
    echo "   nano .env"
    echo ""
    echo "🔧 You need to update:"
    echo "   - MONGO_URI (your MongoDB connection string)"
    echo "   - GCP_PROJECT_ID (your Google Cloud project ID)"
    echo "   - GCS_BUCKET (your storage bucket name)"
    echo ""
else
    echo "✅ .env file already exists"
fi

# Check if virtual environment exists
if [ ! -d "venv" ] && [ ! -d ".venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created!"
    echo ""
    echo "💡 Activate with: source venv/bin/activate"
    echo ""
else
    echo "✅ Virtual environment already exists"
fi

# Check if in virtual environment
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️  Virtual environment not activated"
    echo "💡 Please run: source venv/bin/activate"
    echo "💡 Then run this script again"
    exit 1
fi

# Install dependencies
echo "📦 Installing Python dependencies..."

# Install basic requirements first
if [ -f requirements.txt ]; then
    pip install -r requirements.txt
    echo "✅ Basic requirements installed"
fi

# Ask if user wants Apache Beam
read -p "📊 Do you want to install Apache Beam for the GCS pipeline? (y/n): " install_beam

if [[ $install_beam == "y" || $install_beam == "Y" ]]; then
    if [ -f beam_requirements.txt ]; then
        pip install -r beam_requirements.txt
        echo "✅ Apache Beam requirements installed"
    fi
fi

# Check MongoDB connection
echo ""
echo "🔍 Testing setup..."

if [ -f .env ]; then
    # Load environment variables
    export $(cat .env | xargs)
    
    # Test with Python
    python3 -c "
import os
from dotenv import load_dotenv
load_dotenv()

mongo_uri = os.getenv('MONGO_URI')
if mongo_uri and 'mongodb' in mongo_uri:
    print('✅ MongoDB URI configured')
else:
    print('❌ MongoDB URI not configured properly')

gcp_project = os.getenv('GCP_PROJECT_ID')
if gcp_project and gcp_project != 'your-gcp-project-id':
    print('✅ GCP Project ID configured')
else:
    print('❌ GCP Project ID not configured properly')

gcs_bucket = os.getenv('GCS_BUCKET')
if gcs_bucket and gcs_bucket != 'your-gcs-bucket-name':
    print('✅ GCS Bucket configured')
else:
    print('❌ GCS Bucket not configured properly')
"
fi

echo ""
echo "🎯 Setup Summary:"
echo "=================="
echo "✅ Project files ready"
echo "✅ Dependencies installed"
echo "✅ Environment configuration created"
echo ""
echo "📋 Next Steps:"
echo "1. Edit .env file with your credentials"
echo "2. Test MongoDB connection: python test_pipeline.py --quick"
echo "3. Run MongoDB import: python import_to_mongodb.py"
echo "4. Test pipeline: python test_pipeline.py"
echo "5. Deploy to Dataflow: ./run_dataflow_job.sh"
echo ""
echo "📖 For detailed instructions, see:"
echo "   - SECURITY_SETUP.md"
echo "   - Pipeline_Guide.md"
echo "   - README_Pipeline.md"
echo ""
echo "🎉 Setup complete! Happy data processing!"
