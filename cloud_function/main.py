"""
Google Cloud Function to execute MongoDB Parquet export
This function exports MongoDB collections as Parquet files for use in Google Cloud Workflows
Optimized for parallel processing and maximum performance
"""

import os
import json
import tempfile
import zipfile
import base64
from google.cloud import storage
import functions_framework
from pymongo import MongoClient

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoToGCSExporter:
    """Export MongoDB collections to Google Cloud Storage as Parquet files with parallel processing"""
    
    def __init__(self, mongo_uri: str, database_name: str = "brazilian-ecommerce"):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.client = None
        self.db = None
        self.lock = threading.Lock()
        
    def connect(self) -> bool:
        """Connect to MongoDB with optimized settings"""
        try:
            # Optimize MongoDB connection for high throughput
            self.client = MongoClient(
                self.mongo_uri,
                maxPoolSize=20,  # Increase connection pool size
                minPoolSize=5,
                maxIdleTimeMS=30000,
                socketTimeoutMS=120000,
                connectTimeoutMS=20000,
                serverSelectionTimeoutMS=5000
            )
            self.db = self.client[self.database_name]
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB database: {self.database_name} with optimized settings")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    def export_collection_to_parquet(self, collection_name: str, output_path: str) -> Dict[str, Any]:
        """Export a single collection to Parquet with optimized processing"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            collection = self.db[collection_name]
            total_docs = collection.count_documents({})
            if total_docs == 0:
                logger.warning(f"⚠️ Collection '{collection_name}' is empty")
                return {"success": False, "reason": "empty_collection", "count": 0}
            logger.info(f"📊 Exporting {total_docs:,} documents from '{collection_name}'")
            sample_size = min(1000, total_docs)
            sample_docs = list(collection.find().limit(sample_size))
            if not sample_docs:
                return {"success": False, "reason": "no_sample_docs", "count": 0}
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(self._flatten_document(doc).keys())
            fieldnames = sorted(list(all_fields))
            batch_size = 5000 if total_docs > 100000 else 1000
            processed = 0
            data_rows = []
            cursor = collection.find().batch_size(batch_size)
            for doc in cursor:
                flattened_doc = self._flatten_document(doc)
                row = {field: flattened_doc.get(field, '') for field in fieldnames}
                data_rows.append(row)
                processed += 1
                if processed % (50000 if total_docs > 500000 else 10000) == 0:
                    logger.info(f"    💾 {collection_name}: {processed:,}/{total_docs:,} documents ({processed/total_docs*100:.1f}%)")
            df = pd.DataFrame(data_rows, columns=fieldnames)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)
            file_size = os.path.getsize(output_path)
            logger.info(f"    ✅ Exported {collection_name}: {processed:,} documents ({file_size / (1024*1024):.2f} MB)")
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
    
    def export_collection_parallel_worker(self, collection_name: str, output_dir: str) -> Dict[str, Any]:
        """Worker function for parallel collection export (Parquet)"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            # Create a separate MongoDB connection for this thread
            thread_client = MongoClient(
                self.mongo_uri,
                maxPoolSize=5,
                minPoolSize=1,
                maxIdleTimeMS=30000,
                socketTimeoutMS=120000,
                connectTimeoutMS=20000
            )
            thread_db = thread_client[self.database_name]
            output_path = os.path.join(output_dir, f"{collection_name}.parquet")
            collection = thread_db[collection_name]
            total_docs = collection.count_documents({})
            if total_docs == 0:
                thread_client.close()
                return {
                    "collection": collection_name,
                    "status": "empty",
                    "count": 0,
                    "path": None
                }
            logger.info(f"🔄 [{threading.current_thread().name}] Exporting {collection_name}: {total_docs:,} documents")
            sample_size = min(1000, total_docs)
            sample_docs = list(collection.find().limit(sample_size))
            if not sample_docs:
                thread_client.close()
                return {
                    "collection": collection_name,
                    "status": "error",
                    "error": "No sample docs found"
                }
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(self._flatten_document(doc).keys())
            fieldnames = sorted(list(all_fields))
            start_time = time.time()
            batch_size = 10000 if total_docs > 100000 else 5000
            processed = 0
            data_rows = []
            cursor = collection.find().batch_size(batch_size)
            for doc in cursor:
                flattened_doc = self._flatten_document(doc)
                row = {field: flattened_doc.get(field, '') for field in fieldnames}
                data_rows.append(row)
                processed += 1
                if processed % 50000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed
                    logger.info(f"    📊 [{threading.current_thread().name}] {collection_name}: {processed:,}/{total_docs:,} ({processed/total_docs*100:.1f}%) - {rate:.0f} docs/sec")
            df = pd.DataFrame(data_rows, columns=fieldnames)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)
            elapsed = time.time() - start_time
            file_size = os.path.getsize(output_path)
            rate = processed / elapsed if elapsed > 0 else 0
            logger.info(f"✅ [{threading.current_thread().name}] Completed {collection_name}: {processed:,} docs, {file_size/(1024*1024):.2f} MB, {elapsed:.1f}s, {rate:.0f} docs/sec")
            thread_client.close()
            return {
                "collection": collection_name,
                "status": "success",
                "count": processed,
                "path": output_path,
                "size_mb": file_size / (1024*1024),
                "filename": f"{collection_name}.parquet",
                "duration_seconds": elapsed,
                "rate_docs_per_second": rate
            }
            
        except Exception as e:
            logger.error(f"❌ [{threading.current_thread().name}] Error exporting {collection_name}: {e}")
            return {
                "collection": collection_name,
                "status": "error",
                "error": str(e)
            }
    
    def export_all_collections_parallel(self, output_dir: str, max_workers: int = 4) -> Dict[str, Any]:
        """Export collections in parallel for maximum performance"""
        try:
            # Get all collection names and sizes
            collection_names = self.db.list_collection_names()
            logger.info(f"📚 Found {len(collection_names)} collections: {collection_names}")
            
            # Get collection sizes
            collection_info = []
            for name in collection_names:
                size = self.db[name].count_documents({})
                collection_info.append((name, size))
            
            # Separate small and large collections for optimal processing
            small_collections = [(name, size) for name, size in collection_info if size < 10000]
            large_collections = [(name, size) for name, size in collection_info if size >= 10000]
            
            # Sort large collections by size (largest first to start early)
            large_collections.sort(key=lambda x: x[1], reverse=True)
            small_collections.sort(key=lambda x: x[1])
            
            logger.info(f"🚀 Parallel processing strategy:")
            logger.info(f"   Large collections ({len(large_collections)}): {[name for name, _ in large_collections]}")
            logger.info(f"   Small collections ({len(small_collections)}): {[name for name, _ in small_collections]}")
            
            results = []
            successful = 0
            failed = 0
            total_processed = 0
            start_time = time.time()
            
            # Process large collections first (they take longest)
            if large_collections:
                logger.info(f"🔄 Processing {len(large_collections)} large collections with {min(max_workers, len(large_collections))} workers")
                
                with ThreadPoolExecutor(max_workers=min(max_workers, len(large_collections))) as executor:
                    # Submit large collection jobs
                    future_to_collection = {
                        executor.submit(self.export_collection_parallel_worker, name, output_dir): name 
                        for name, _ in large_collections
                    }
                    
                    # Process results as they complete
                    for future in as_completed(future_to_collection):
                        collection_name = future_to_collection[future]
                        try:
                            result = future.result()
                            results.append(result)
                            
                            if result["status"] == "success":
                                successful += 1
                                total_processed += result["count"]
                            else:
                                failed += 1
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing {collection_name}: {e}")
                            results.append({
                                "collection": collection_name,
                                "status": "error",
                                "error": str(e)
                            })
                            failed += 1
            
            # Process small collections (can use more workers since they're fast)
            if small_collections:
                logger.info(f"🔄 Processing {len(small_collections)} small collections with {max_workers} workers")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_collection = {
                        executor.submit(self.export_collection_parallel_worker, name, output_dir): name 
                        for name, _ in small_collections
                    }
                    
                    for future in as_completed(future_to_collection):
                        collection_name = future_to_collection[future]
                        try:
                            result = future.result()
                            results.append(result)
                            
                            if result["status"] == "success":
                                successful += 1
                                total_processed += result["count"]
                            else:
                                failed += 1
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing {collection_name}: {e}")
                            results.append({
                                "collection": collection_name,
                                "status": "error",
                                "error": str(e)
                            })
                            failed += 1
            
            total_time = time.time() - start_time
            overall_rate = total_processed / total_time if total_time > 0 else 0
            
            logger.info(f"🎉 Parallel export completed: {successful} successful, {failed} failed")
            logger.info(f"📊 Total: {total_processed:,} documents in {total_time:.1f}s ({overall_rate:.0f} docs/sec)")
            
            return {
                "total_collections": len(collection_names),
                "successful": successful,
                "failed": failed,
                "total_documents_processed": total_processed,
                "total_duration_seconds": total_time,
                "overall_rate_docs_per_second": overall_rate,
                "results": results
            }
            
        except Exception as e:
            logger.error(f"❌ Error in parallel export: {e}")
            return {
                "total_collections": 0,
                "successful": 0,
                "failed": 1,
                "results": [],
                "error": str(e)
            }
    
    def export_all_collections(self, output_dir: str) -> Dict[str, Any]:
        """Export all collections to Parquet files with optimized ordering"""
        try:
            # Get all collection names
            collection_names = self.db.list_collection_names()
            logger.info(f"📚 Found {len(collection_names)} collections: {collection_names}")
            # Get collection sizes and sort by size (smallest first for faster feedback)
            collection_sizes = []
            for name in collection_names:
                size = self.db[name].count_documents({})
                collection_sizes.append((name, size))
            # Sort by size (smallest first)
            collection_sizes.sort(key=lambda x: x[1])
            logger.info(f"📊 Processing order (by size):")
            for name, size in collection_sizes:
                logger.info(f"  - {name}: {size:,} documents")
            results = []
            successful = 0
            failed = 0
            total_processed = 0
            for collection_name, expected_count in collection_sizes:
                logger.info(f"🔄 Starting export of '{collection_name}' ({expected_count:,} documents)")
                output_path = os.path.join(output_dir, f"{collection_name}.parquet")
                result = self.export_collection_to_parquet(collection_name, output_path)
                # Convert the result format to match what the function expects
                if result["success"]:
                    results.append({
                        "collection": collection_name,
                        "status": "success",
                        "count": result["count"],
                        "path": output_path,
                        "size_mb": result["size_mb"],
                        "filename": f"{collection_name}.parquet"
                    })
                    successful += 1
                    total_processed += result["count"]
                    logger.info(f"✅ Completed {collection_name}: {result['count']:,} docs, {result['size_mb']:.2f} MB")
                else:
                    results.append({
                        "collection": collection_name,
                        "status": "error",
                        "error": result.get("error", "Unknown error")
                    })
                    failed += 1
                    logger.error(f"❌ Failed {collection_name}: {result.get('error', 'Unknown error')}")
                # Progress update
                logger.info(f"📈 Progress: {successful + failed}/{len(collection_names)} collections, {total_processed:,} total documents processed")
            return {
                "total_collections": len(collection_names),
                "successful": successful,
                "failed": failed,
                "total_documents_processed": total_processed,
                "results": results
            }
        except Exception as e:
            logger.error(f"❌ Error in export_all_collections: {e}")
            return {
                "total_collections": 0,
                "successful": 0,
                "failed": 1,
                "results": [],
                "error": str(e)
            }
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")

@functions_framework.http
def main(request):
    """
    Google Cloud Function entry point
    """
    try:
        # Get environment variables
        mongo_uri = os.getenv('MONGO_URI')
        database_name = os.getenv('DATABASE_NAME', 'brazilian-ecommerce')
        bucket_name = os.getenv('BUCKET_NAME', 'my_bec_bucket')
        
        if not mongo_uri:
            return {"error": "MONGO_URI environment variable not set"}, 500
        
    # Create temporary directory for Parquet files
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"📁 Using temporary directory: {temp_dir}")
            
            # Initialize exporter
            exporter = MongoToGCSExporter(mongo_uri, database_name)
            
            if not exporter.connect():
                return {"error": "Failed to connect to MongoDB"}, 500
            
            # Export collections using parallel processing for maximum speed
            logger.info("🚀 Starting PARALLEL export for maximum performance")
            export_summary = exporter.export_all_collections_parallel(temp_dir, max_workers=6)
            logger.info(f"📊 Parallel export summary: {export_summary}")
            
            # Upload to GCS using parallel uploads for maximum speed
            if export_summary['successful'] > 0:
                successful_exports = [r for r in export_summary['results'] if r['status'] == 'success']
                logger.info("🚀 Starting PARALLEL upload to GCS")
                uploaded_files = upload_to_gcs_parallel(successful_exports, bucket_name, "")
                
                response = {
                    "status": "success",
                    "message": f"PARALLEL: Exported {export_summary['successful']} collections to GCS",
                    "files": uploaded_files,
                    "summary": export_summary,
                    "performance": {
                        "total_documents": export_summary.get('total_documents_processed', 0),
                        "total_duration_seconds": export_summary.get('total_duration_seconds', 0),
                        "overall_rate_docs_per_second": export_summary.get('overall_rate_docs_per_second', 0)
                    }
                }
            else:
                response = {
                    "status": "error",
                    "message": "No collections were successfully exported",
                    "summary": export_summary
                }
        
        exporter.close()
        return response, 200
        
    except Exception as e:
        logger.error(f"Function execution failed: {e}")
        return {"error": f"Function failed: {str(e)}"}, 500

def upload_file_to_gcs(file_info: Dict, bucket_name: str, folder_name: str) -> Dict:
    """Upload a single file to GCS (worker function for parallel uploads)"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        if folder_name:
            blob_name = f"{folder_name}/{file_info['filename']}"
        else:
            blob_name = file_info['filename']
        
        blob = bucket.blob(blob_name)
        
        # Upload with optimized settings
        start_time = time.time()
        with open(file_info["path"], 'rb') as f:
            blob.upload_from_file(f, content_type='application/octet-stream')
        
        upload_time = time.time() - start_time
        
        result = {
            "collection": file_info["collection"],
            "filename": file_info["filename"],
            "gcs_path": f"gs://{bucket_name}/{blob_name}",
            "size_mb": file_info["size_mb"],
            "count": file_info["count"],
            "upload_time_seconds": upload_time
        }
        
        logger.info(f"📁 [{threading.current_thread().name}] Uploaded {file_info['filename']} ({file_info['size_mb']:.2f}MB) in {upload_time:.1f}s")
        return result
        
    except Exception as e:
        logger.error(f"❌ Upload failed for {file_info.get('filename', 'unknown')}: {e}")
        raise e

def upload_to_gcs_parallel(exported_files: List[Dict], bucket_name: str, folder_name: str) -> List[Dict]:
    """Upload exported Parquet files to Google Cloud Storage in parallel"""
    try:
        logger.info(f"🚀 Starting parallel upload of {len(exported_files)} files to GCS")
        start_time = time.time()
        uploaded_files = []
        
        # Use parallel uploads for better performance
        max_upload_workers = min(6, len(exported_files))
        
        with ThreadPoolExecutor(max_workers=max_upload_workers) as executor:
            future_to_file = {
                executor.submit(upload_file_to_gcs, file_info, bucket_name, folder_name): file_info
                for file_info in exported_files
            }
            
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                try:
                    result = future.result()
                    uploaded_files.append(result)
                except Exception as e:
                    logger.error(f"❌ Failed to upload {file_info.get('filename', 'unknown')}: {e}")
                    # Continue with other uploads
        
        total_time = time.time() - start_time
        total_size = sum(f['size_mb'] for f in uploaded_files)
        
        logger.info(f"✅ Parallel upload completed: {len(uploaded_files)} files, {total_size:.2f}MB in {total_time:.1f}s")
        
        return uploaded_files
        
    except Exception as e:
        logger.error(f"GCS parallel upload failed: {e}")
        raise e

def upload_to_gcs(exported_files: List[Dict], bucket_name: str, folder_name: str) -> List[Dict]:
    """Upload exported Parquet files to Google Cloud Storage (fallback sequential method)"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        uploaded_files = []
        
        for file_info in exported_files:
            # If folder_name is empty, upload directly to bucket root
            if folder_name:
                blob_name = f"{folder_name}/{file_info['filename']}"
            else:
                blob_name = file_info['filename']
            
            blob = bucket.blob(blob_name)
            
            # Upload file
            with open(file_info["path"], 'rb') as f:
                blob.upload_from_file(f, content_type='application/octet-stream')
            
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
