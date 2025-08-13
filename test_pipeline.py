#!/usr/bin/env python3
"""
Local Test Script for MongoDB to GCS Pipeline
Tests the pipeline components locally before deploying to Dataflow
"""

import pandas as pd
import pymongo
from pymongo import MongoClient
import json
import logging
import os
from datetime import datetime
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LocalPipelineTest:
    """
    Local test class to validate pipeline components
    """
    
    def __init__(self, mongo_uri=None, database_name=None):
        # Use environment variables if not provided
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI')
        self.database_name = database_name or os.getenv('MONGO_DATABASE', 'brazilian-ecommerce')
        
        if not self.mongo_uri:
            raise ValueError("MongoDB connection string not provided. Set MONGO_URI environment variable or pass mongo_uri parameter.")
        
        self.client = None
        self.db = None
        
    def connect_mongodb(self):
        """Test MongoDB connection"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            logger.info(f"✅ Successfully connected to MongoDB: {self.database_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
    
    def test_collection_access(self):
        """Test access to all collections"""
        collections = [
            'customers', 'orders', 'order_items', 'order_payments', 
            'order_reviews', 'products', 'sellers', 'geolocation', 
            'product_categories'
        ]
        
        logger.info("🔍 Testing collection access...")
        available_collections = self.db.list_collection_names()
        
        results = {}
        for collection_name in collections:
            if collection_name in available_collections:
                try:
                    count = self.db[collection_name].count_documents({})
                    results[collection_name] = {
                        'status': 'available',
                        'count': count
                    }
                    logger.info(f"   ✅ {collection_name}: {count:,} documents")
                except Exception as e:
                    results[collection_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    logger.error(f"   ❌ {collection_name}: Error - {str(e)}")
            else:
                results[collection_name] = {
                    'status': 'missing',
                    'count': 0
                }
                logger.warning(f"   ⚠️  {collection_name}: Collection not found")
        
        return results
    
    def test_data_conversion(self, collection_name="customers", limit=5):
        """Test converting MongoDB documents to CSV format"""
        logger.info(f"🧪 Testing data conversion for '{collection_name}' collection...")
        
        try:
            collection = self.db[collection_name]
            documents = list(collection.find({}).limit(limit))
            
            if not documents:
                logger.warning(f"   ⚠️  No documents found in '{collection_name}'")
                return False
            
            # Test flattening documents
            flattened_docs = []
            for doc in documents:
                flat_doc = self._flatten_document(doc)
                flattened_docs.append(flat_doc)
            
            # Convert to DataFrame for CSV preview
            df = pd.DataFrame(flattened_docs)
            
            logger.info(f"   ✅ Successfully converted {len(documents)} documents")
            logger.info(f"   📊 Columns: {list(df.columns)}")
            
            # Save sample CSV
            output_file = f"test_{collection_name}_sample.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"   💾 Sample CSV saved: {output_file}")
            
            # Show sample data
            print(f"\n📋 Sample data from {collection_name}:")
            print(df.head())
            print(f"\n📐 Shape: {df.shape}")
            
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Failed to convert data: {str(e)}")
            return False
    
    def _flatten_document(self, doc, prefix=''):
        """Flatten nested MongoDB document"""
        flattened = {}
        
        for key, value in doc.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict):
                flattened.update(self._flatten_document(value, new_key))
            elif isinstance(value, list):
                flattened[new_key] = json.dumps(value) if value else ''
            else:
                # Convert ObjectId to string
                if hasattr(value, '__str__'):
                    flattened[new_key] = str(value)
                else:
                    flattened[new_key] = value
        
        return flattened
    
    def estimate_data_sizes(self):
        """Estimate data sizes for cost planning"""
        logger.info("📊 Estimating data sizes...")
        
        collections_info = self.test_collection_access()
        total_documents = 0
        
        print(f"\n{'Collection':<20} {'Documents':<12} {'Est. Size':<12}")
        print("=" * 50)
        
        for collection_name, info in collections_info.items():
            if info['status'] == 'available':
                count = info['count']
                total_documents += count
                
                # Estimate size (rough calculation)
                # Average MongoDB document ~1-2KB, CSV might be 0.5-1KB
                estimated_size_mb = (count * 1) / 1024  # Rough estimate in MB
                
                print(f"{collection_name:<20} {count:<12,} {estimated_size_mb:<8.1f} MB")
        
        print("=" * 50)
        print(f"{'TOTAL':<20} {total_documents:<12,} {(total_documents * 1) / 1024:<8.1f} MB")
        
        # Dataflow cost estimation
        estimated_cost = self._estimate_dataflow_cost(total_documents)
        print(f"\n💰 Estimated Dataflow cost: ${estimated_cost:.2f}")
        
        return total_documents
    
    def _estimate_dataflow_cost(self, total_documents):
        """Rough cost estimation for Dataflow job"""
        # Very rough estimation based on document count
        # Actual costs depend on machine type, region, duration
        if total_documents < 100000:
            return 2.0
        elif total_documents < 1000000:
            return 5.0
        elif total_documents < 5000000:
            return 15.0
        else:
            return 30.0
    
    def generate_test_pipeline_command(self):
        """Generate command for local pipeline testing"""
        logger.info("🔧 Generating test pipeline command...")
        
        command = """
# Test pipeline locally with DirectRunner
python mongodb_to_gcs_pipeline.py \\
    --project=test-project \\
    --gcs_bucket=test-bucket \\
    --runner=DirectRunner \\
    --collections=customers \\
    --temp_location=./temp \\
    --staging_location=./staging
"""
        
        print("\n🚀 Local Test Command:")
        print("=" * 50)
        print(command)
        
        return command
    
    def cleanup_test_files(self):
        """Clean up test files"""
        test_files = [f for f in os.listdir('.') if f.startswith('test_') and f.endswith('.csv')]
        
        if test_files:
            logger.info(f"🧹 Cleaning up {len(test_files)} test files...")
            for file in test_files:
                os.remove(file)
                logger.info(f"   🗑️  Removed: {file}")
        else:
            logger.info("🧹 No test files to clean up")
    
    def run_full_test(self):
        """Run complete test suite"""
        logger.info("🎯 Starting full pipeline test suite...")
        print("=" * 60)
        
        # Test 1: MongoDB Connection
        if not self.connect_mongodb():
            logger.error("❌ Test suite failed: Cannot connect to MongoDB")
            return False
        
        # Test 2: Collection Access
        collections_info = self.test_collection_access()
        available_collections = [name for name, info in collections_info.items() 
                               if info['status'] == 'available']
        
        if not available_collections:
            logger.error("❌ Test suite failed: No collections available")
            return False
        
        # Test 3: Data Conversion
        test_collection = available_collections[0]  # Test with first available collection
        if not self.test_data_conversion(test_collection):
            logger.error(f"❌ Test suite failed: Data conversion failed for {test_collection}")
            return False
        
        # Test 4: Size Estimation
        total_docs = self.estimate_data_sizes()
        
        # Test 5: Generate Commands
        self.generate_test_pipeline_command()
        
        # Summary
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
        print("=" * 60)
        print(f"📊 Available collections: {len(available_collections)}")
        print(f"📈 Total documents: {total_docs:,}")
        print(f"🎯 Ready for Dataflow deployment!")
        
        return True

def main():
    """Main test execution"""
    tester = LocalPipelineTest()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--cleanup':
            tester.cleanup_test_files()
            return
        elif sys.argv[1] == '--quick':
            # Quick test - just connection and one collection
            if tester.connect_mongodb():
                tester.test_collection_access()
            return
    
    # Run full test suite
    success = tester.run_full_test()
    
    if success:
        print(f"\n💡 Next steps:")
        print(f"   1. Update your project ID in run_dataflow_job.sh")
        print(f"   2. Create a GCS bucket")
        print(f"   3. Run: ./run_dataflow_job.sh")
    else:
        print(f"\n❌ Please fix the issues above before proceeding")
        sys.exit(1)

if __name__ == "__main__":
    main()
