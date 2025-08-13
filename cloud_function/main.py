"""
Google Cloud Function to execute MongoDB CSV export
This function wraps our simple_csv_export.py script for use in Google Cloud Workflows
"""

import os
import json
import tempfile
import zipfile
import base64
from google.cloud import storage
import functions_framework
from pymongo import MongoClient
import csv
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoToGCSExporter:
    """Export MongoDB collections to Google Cloud Storage as CSV files"""
    
    def __init__(self, mongo_uri: str, database_name: str = "brazilian-ecommerce"):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.client = None
        self.db = None
        
    def connect(self) -> bool:
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB database: {self.database_name}")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    def export_collection_to_csv(self, collection_name: str, output_path: str) -> Dict[str, Any]:
        """Export a single collection to CSV"""
        try:
            collection = self.db[collection_name]
            total_docs = collection.count_documents({})
            
            if total_docs == 0:
                logger.warning(f"⚠️ Collection '{collection_name}' is empty")
                return {"success": False, "reason": "empty_collection", "count": 0}
            
            logger.info(f"📊 Exporting {total_docs:,} documents from '{collection_name}'")
            
            # Get field names from sample documents
            sample_docs = list(collection.find().limit(100))
            if not sample_docs:
                return {"success": False, "reason": "no_sample_docs", "count": 0}
            
            # Extract all unique field names
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(self._flatten_document(doc).keys())
            
            fieldnames = sorted(list(all_fields))
            
            # Write CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                batch_size = 1000
                processed = 0
                
                for doc in collection.find().batch_size(batch_size):
                    flattened_doc = self._flatten_document(doc)
                    # Ensure all fields are present
                    row = {field: flattened_doc.get(field, '') for field in fieldnames}
                    writer.writerow(row)
                    
                    processed += 1
                    if processed % 10000 == 0:
                        logger.info(f"    💾 Processed: {processed:,}/{total_docs:,} documents")
            
            file_size = os.path.getsize(output_path)
            logger.info(f"    ✅ Exported {processed:,} documents ({file_size / (1024*1024):.2f} MB)")
            
            return {
                "success": True, 
                "count": processed, 
                "size_bytes": file_size,
                "size_mb": file_size / (1024*1024)
            }
            
        except Exception as e:
            logger.error(f"❌ Error exporting collection '{collection_name}': {e}")
            return {"success": False, "error": str(e), "count": 0}
    
    def _flatten_document(self, doc: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested MongoDB document"""
        items = []
        for k, v in doc.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_document(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert list to string representation
                items.append((new_key, str(v) if v else ''))
            else:
                # Convert ObjectId and other types to string
                items.append((new_key, str(v) if v is not None else ''))
        
        return dict(items)
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")

@functions_framework.http
def mongodb_csv_exporter(request):
    """
    Google Cloud Function entry point
    Expected request body:
    {
        "export_folder": "exports_20240813_120000",
        "collections": ["customers", "orders", "products"],
        "bucket_name": "my-gcs-bucket" (optional)
    }
    """
    try:
        # Parse request
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "No JSON payload provided"}, 400
        
        export_folder = request_json.get('export_folder', 'exports')
        collections = request_json.get('collections', [
            "customers", "sellers", "orders", "order_items",
            "order_payments", "order_reviews", "products", 
            "geolocation", "product_categories"
        ])
        bucket_name = request_json.get('bucket_name')
        
        # Get MongoDB URI from environment
        mongo_uri = os.getenv('MONGO_URI')
        if not mongo_uri:
            return {"error": "MONGO_URI environment variable not set"}, 500
        
        # Initialize exporter
        exporter = MongoToGCSExporter(mongo_uri)
        if not exporter.connect():
            return {"error": "Failed to connect to MongoDB"}, 500
        
        # Create temporary directory for exports
        with tempfile.TemporaryDirectory() as temp_dir:
            exported_files = []
            export_summary = {"successful": 0, "failed": 0, "total_documents": 0}
            
            # Export each collection
            for collection_name in collections:
                csv_path = os.path.join(temp_dir, f"{collection_name}.csv")
                result = exporter.export_collection_to_csv(collection_name, csv_path)
                
                if result["success"]:
                    exported_files.append({
                        "collection": collection_name,
                        "filename": f"{collection_name}.csv",
                        "path": csv_path,
                        "count": result["count"],
                        "size_bytes": result["size_bytes"],
                        "size_mb": result["size_mb"]
                    })
                    export_summary["successful"] += 1
                    export_summary["total_documents"] += result["count"]
                else:
                    export_summary["failed"] += 1
                    logger.error(f"Failed to export {collection_name}: {result}")
            
            # Upload to Google Cloud Storage if bucket specified
            if bucket_name:
                gcs_files = upload_to_gcs(exported_files, bucket_name, export_folder)
                response = {
                    "status": "success",
                    "message": f"Exported {export_summary['successful']} collections to GCS",
                    "bucket": bucket_name,
                    "folder": export_folder,
                    "files": gcs_files,
                    "summary": export_summary
                }
            else:
                # Return file contents as base64 for workflow processing
                file_contents = []
                for file_info in exported_files:
                    with open(file_info["path"], 'rb') as f:
                        content = base64.b64encode(f.read()).decode('utf-8')
                        file_contents.append({
                            "filename": file_info["filename"],
                            "content": content,
                            "size": f"{file_info['size_mb']:.2f} MB",
                            "count": file_info["count"]
                        })
                
                response = {
                    "status": "success",
                    "message": f"Exported {export_summary['successful']} collections",
                    "files": file_contents,
                    "summary": export_summary
                }
        
        exporter.close()
        return response, 200
        
    except Exception as e:
        logger.error(f"Function execution failed: {e}")
        return {"error": f"Function failed: {str(e)}"}, 500

def upload_to_gcs(exported_files: List[Dict], bucket_name: str, folder_name: str) -> List[Dict]:
    """Upload exported CSV files to Google Cloud Storage"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        uploaded_files = []
        
        for file_info in exported_files:
            blob_name = f"{folder_name}/{file_info['filename']}"
            blob = bucket.blob(blob_name)
            
            # Upload file
            with open(file_info["path"], 'rb') as f:
                blob.upload_from_file(f, content_type='text/csv')
            
            uploaded_files.append({
                "collection": file_info["collection"],
                "filename": file_info["filename"],
                "gcs_path": f"gs://{bucket_name}/{blob_name}",
                "size_mb": file_info["size_mb"],
                "count": file_info["count"]
            })
            
            logger.info(f"📁 Uploaded {file_info['filename']} to gs://{bucket_name}/{blob_name}")
        
        return uploaded_files
        
    except Exception as e:
        logger.error(f"GCS upload failed: {e}")
        raise e
