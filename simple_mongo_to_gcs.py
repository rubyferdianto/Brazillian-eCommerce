#!/usr/bin/env python3
"""
Simple MongoDB to Google Cloud Storage Export (without Apache Beam)
Alternative solution for Python 3.12+ or when Apache Beam is not available
"""

import pandas as pd
import pymongo
from pymongo import MongoClient
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import storage
import tempfile
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleMongoToGCSExporter:
    """
    Simple exporter that reads from MongoDB and uploads CSV files to Google Cloud Storage
    """
    
    def __init__(self):
        self.mongo_uri = os.getenv('MONGO_URI')
        self.mongo_database = os.getenv('MONGO_DATABASE', 'brazilian-ecommerce')
        self.gcp_project_id = os.getenv('GCP_PROJECT_ID')
        self.gcs_bucket = os.getenv('GCS_BUCKET')
        
        # Validate environment variables
        if not self.mongo_uri:
            raise ValueError("MONGO_URI environment variable not set")
        if not self.gcp_project_id:
            raise ValueError("GCP_PROJECT_ID environment variable not set")
        if not self.gcs_bucket:
            raise ValueError("GCS_BUCKET environment variable not set")
        
        self.client = None
        self.db = None
        self.storage_client = None
        
    def connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.mongo_database]
            logger.info(f"✅ Connected to MongoDB: {self.mongo_database}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
    
    def connect_gcs(self):
        """Connect to Google Cloud Storage"""
        try:
            self.storage_client = storage.Client(project=self.gcp_project_id)
            # Test connection by listing bucket
            bucket = self.storage_client.bucket(self.gcs_bucket)
            bucket.reload()
            logger.info(f"✅ Connected to GCS bucket: {self.gcs_bucket}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to GCS: {str(e)}")
            logger.error("💡 Make sure you're authenticated: gcloud auth application-default login")
            return False
    
    def flatten_document(self, doc, prefix=''):
        """Flatten nested MongoDB document for CSV conversion"""
        flattened = {}
        
        for key, value in doc.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict):
                flattened.update(self.flatten_document(value, new_key))
            elif isinstance(value, list):
                flattened[new_key] = json.dumps(value) if value else ''
            else:
                # Convert ObjectId and other types to string
                flattened[new_key] = str(value) if value is not None else ''
        
        return flattened
    
    def export_collection_to_csv(self, collection_name, limit=None):
        """Export a MongoDB collection to CSV and upload to GCS"""
        try:
            logger.info(f"📊 Exporting collection: {collection_name}")
            
            # Get collection
            collection = self.db[collection_name]
            
            # Count documents
            total_docs = collection.count_documents({})
            if total_docs == 0:
                logger.warning(f"⚠️  Collection '{collection_name}' is empty")
                return False
            
            # Apply limit if specified
            cursor = collection.find({}).limit(limit) if limit else collection.find({})
            
            # Process documents in batches
            batch_size = 10000
            documents = []
            processed = 0
            
            for doc in cursor:
                # Flatten document
                flat_doc = self.flatten_document(doc)
                documents.append(flat_doc)
                processed += 1
                
                # Process batch
                if len(documents) >= batch_size:
                    if not self._process_batch(documents, collection_name, processed, total_docs):
                        return False
                    documents = []
            
            # Process remaining documents
            if documents:
                if not self._process_batch(documents, collection_name, processed, total_docs):
                    return False
            
            logger.info(f"✅ Successfully exported {processed:,} documents from '{collection_name}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to export collection '{collection_name}': {str(e)}")
            return False
    
    def _process_batch(self, documents, collection_name, processed, total_docs):
        """Process a batch of documents"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(documents)
            
            # Create temporary CSV file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
                df.to_csv(tmp_file.name, index=False)
                csv_path = tmp_file.name
            
            # Upload to GCS
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blob_name = f"brazilian-ecommerce-exports/{collection_name}.csv"
            blob = bucket.blob(blob_name)
            
            # Upload file
            blob.upload_from_filename(csv_path)
            
            # Clean up temporary file
            os.unlink(csv_path)
            
            logger.info(f"   📤 Uploaded batch to gs://{self.gcs_bucket}/{blob_name}")
            logger.info(f"   📈 Progress: {processed:,}/{total_docs:,} documents")
            
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Failed to process batch: {str(e)}")
            return False
    
    def export_all_collections(self, collections=None, limit=None):
        """Export all specified collections"""
        if collections is None:
            collections = [
                'customers', 'orders', 'order_items', 'order_payments',
                'order_reviews', 'products', 'sellers', 'geolocation',
                'product_categories'
            ]
        
        logger.info(f"🚀 Starting export of {len(collections)} collections...")
        
        success_count = 0
        failed_collections = []
        
        for collection_name in collections:
            if collection_name in self.db.list_collection_names():
                if self.export_collection_to_csv(collection_name, limit):
                    success_count += 1
                else:
                    failed_collections.append(collection_name)
            else:
                logger.warning(f"⚠️  Collection '{collection_name}' not found in database")
                failed_collections.append(collection_name)
        
        # Summary
        logger.info(f"\n📊 Export Summary:")
        logger.info(f"✅ Successful: {success_count}/{len(collections)} collections")
        
        if failed_collections:
            logger.warning(f"❌ Failed: {', '.join(failed_collections)}")
        
        logger.info(f"📂 Files available at: gs://{self.gcs_bucket}/brazilian-ecommerce-exports/")
        
        return success_count == len(collections)
    
    def list_gcs_files(self):
        """List exported files in GCS"""
        try:
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blobs = bucket.list_blobs(prefix="brazilian-ecommerce-exports/")
            
            logger.info(f"\n📁 Files in gs://{self.gcs_bucket}/brazilian-ecommerce-exports/:")
            for blob in blobs:
                size_mb = blob.size / (1024 * 1024) if blob.size else 0
                logger.info(f"   📄 {blob.name.split('/')[-1]:<25} {size_mb:>8.2f} MB")
                
        except Exception as e:
            logger.error(f"❌ Failed to list GCS files: {str(e)}")

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Export MongoDB collections to Google Cloud Storage")
    parser.add_argument('--collections', help='Comma-separated list of collections to export')
    parser.add_argument('--limit', type=int, help='Limit number of documents per collection (for testing)')
    parser.add_argument('--list-files', action='store_true', help='List files in GCS bucket')
    
    args = parser.parse_args()
    
    # Create exporter
    exporter = SimpleMongoToGCSExporter()
    
    # Connect to services
    if not exporter.connect_mongodb():
        sys.exit(1)
    
    if not exporter.connect_gcs():
        sys.exit(1)
    
    # List files if requested
    if args.list_files:
        exporter.list_gcs_files()
        return
    
    # Parse collections
    collections = None
    if args.collections:
        collections = [col.strip() for col in args.collections.split(',')]
    
    # Export collections
    success = exporter.export_all_collections(collections, args.limit)
    
    if success:
        logger.info(f"\n🎉 Export completed successfully!")
        exporter.list_gcs_files()
    else:
        logger.error(f"\n❌ Export completed with errors")
        sys.exit(1)

if __name__ == "__main__":
    main()
