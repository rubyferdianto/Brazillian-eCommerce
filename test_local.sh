#!/bin/bash

# Local testing script for the Cloud Function
# This allows you to test the function locally before deploying

echo "🧪 Testing MongoDB CSV Export Function Locally"
echo "="

# Check if functions-framework is installed
if ! python -c "import functions_framework" 2>/dev/null; then
    echo "📦 Installing functions-framework..."
    pip install functions-framework
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️ Warning: .env file not found!"
    echo "💡 Create a .env file with your MONGO_URI"
    echo "   Example: MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/"
    exit 1
fi

# Load environment variables
export $(cat .env | xargs)

echo "🚀 Starting local function server..."
echo "📡 Function will be available at: http://localhost:8080"
echo "🔄 Use Ctrl+C to stop"
echo ""

# Start the function locally
cd cloud_function
functions-framework --target=mongodb_csv_exporter --debug

echo "✅ Local testing server stopped"
