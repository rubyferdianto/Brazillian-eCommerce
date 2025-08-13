#!/usr/bin/env python3
"""
Simple MongoDB to CSV Exporter (No Pandas Required)
Works with Python 3.13 - minimal dependencies
"""

import pymongo
from pymongo import MongoClient
import csv
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleMongoExporter:
    """
    Simple MongoDB to CSV exporter without heavy dependencies
    """
    
    def __init__(self):
        self.mongo_uri = os.getenv('MONGO_URI')
        self.mongo_database = os.getenv('MONGO_DATABASE', 'brazilian-ecommerce')
        
        if not self.mongo_uri:
            raise ValueError("MONGO_URI environment variable not set")
        
        self.client = None
        self.db = None
        
    def connect(self):
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
    
    def flatten_document(self, doc, prefix='', max_depth=3, current_depth=0):
        """
        Flatten nested MongoDB document
        Limited depth to prevent excessive nesting
        """
        flattened = {}
        
        if current_depth >= max_depth:
            # Convert deeply nested objects to JSON strings
            for key, value in doc.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened[new_key] = json.dumps(value) if value else ''
                else:
                    flattened[new_key] = str(value) if value is not None else ''
            return flattened
        
        for key, value in doc.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict) and value:
                # Recursively flatten dictionaries
                flattened.update(self.flatten_document(value, new_key, max_depth, current_depth + 1))
            elif isinstance(value, list):
                # Convert lists to JSON strings
                flattened[new_key] = json.dumps(value) if value else ''
            else:
                # Handle primitive types
                flattened[new_key] = str(value) if value is not None else ''
        
        return flattened
    
    def export_collection_to_csv(self, collection_name, output_dir='exports', limit=None):
        """Export a MongoDB collection to CSV"""
        try:
            logger.info(f"📊 Exporting collection: {collection_name}")
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Get collection
            collection = self.db[collection_name]
            
            # Count documents
            total_docs = collection.count_documents({})
            if total_docs == 0:
                logger.warning(f"⚠️  Collection '{collection_name}' is empty")
                return False
            
            # Apply limit if specified
            cursor = collection.find({}).limit(limit) if limit else collection.find({})
            
            # Process documents
            csv_file_path = os.path.join(output_dir, f"{collection_name}.csv")
            processed = 0
            all_fieldnames = set()
            documents = []
            
            # First pass: collect all documents and determine all possible fieldnames
            logger.info(f"   📋 Analyzing document structure...")
            for doc in cursor:
                flat_doc = self.flatten_document(doc)
                documents.append(flat_doc)
                all_fieldnames.update(flat_doc.keys())
                processed += 1
                
                if processed % 1000 == 0:
                    logger.info(f"   📈 Analyzed: {processed:,}/{total_docs:,} documents")
            
            # Sort fieldnames for consistent output
            fieldnames = sorted(list(all_fieldnames))
            
            # Second pass: write to CSV
            logger.info(f"   💾 Writing to CSV: {csv_file_path}")
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                
                for i, doc in enumerate(documents):
                    # Ensure all fields are present
                    row = {field: doc.get(field, '') for field in fieldnames}
                    writer.writerow(row)
                    
                    if (i + 1) % 1000 == 0:
                        logger.info(f"   💾 Written: {i + 1:,}/{len(documents):,} documents")
            
            file_size = os.path.getsize(csv_file_path) / (1024 * 1024)  # MB
            logger.info(f"   ✅ Successfully exported {processed:,} documents to '{csv_file_path}' ({file_size:.2f} MB)")
            
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Failed to export collection '{collection_name}': {str(e)}")
            return False
    
    def export_all_collections(self, collections=None, output_dir='exports', limit=None):
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
        
        available_collections = self.db.list_collection_names()
        
        for collection_name in collections:
            if collection_name in available_collections:
                if self.export_collection_to_csv(collection_name, output_dir, limit):
                    success_count += 1
                else:
                    failed_collections.append(collection_name)
                print()  # Empty line for readability
            else:
                logger.warning(f"⚠️  Collection '{collection_name}' not found in database")
                failed_collections.append(collection_name)
        
        # Summary
        logger.info(f"📊 Export Summary:")
        logger.info(f"✅ Successful: {success_count}/{len(collections)} collections")
        
        if failed_collections:
            logger.warning(f"❌ Failed: {', '.join(failed_collections)}")
        
        logger.info(f"📂 Files saved to: {os.path.abspath(output_dir)}/")
        
        return success_count == len(collections)
    
    def list_collections(self):
        """List available collections in the database"""
        try:
            collections = self.db.list_collection_names()
            logger.info(f"📋 Available collections in '{self.mongo_database}':")
            
            for collection_name in sorted(collections):
                count = self.db[collection_name].count_documents({})
                logger.info(f"   📁 {collection_name:<25} | {count:>8,} documents")
            
            return collections
            
        except Exception as e:
            logger.error(f"❌ Failed to list collections: {str(e)}")
            return []

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Export MongoDB collections to CSV files")
    parser.add_argument('--collections', help='Comma-separated list of collections to export')
    parser.add_argument('--output-dir', default='exports', help='Output directory for CSV files')
    parser.add_argument('--limit', type=int, help='Limit number of documents per collection (for testing)')
    parser.add_argument('--list', action='store_true', help='List available collections')
    
    args = parser.parse_args()
    
    # Create exporter
    exporter = SimpleMongoExporter()
    
    # Connect to MongoDB
    if not exporter.connect():
        sys.exit(1)
    
    # List collections if requested
    if args.list:
        exporter.list_collections()
        return
    
    # Parse collections
    collections = None
    if args.collections:
        collections = [col.strip() for col in args.collections.split(',')]
    
    # Export collections
    success = exporter.export_all_collections(collections, args.output_dir, args.limit)
    
    if success:
        logger.info(f"\n🎉 Export completed successfully!")
        logger.info(f"📂 Check the '{args.output_dir}' directory for your CSV files")
    else:
        logger.error(f"\n❌ Export completed with errors")
        sys.exit(1)

if __name__ == "__main__":
    main()
