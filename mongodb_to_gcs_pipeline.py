#!/usr/bin/env python3
"""
Apache Beam Pipeline: MongoDB to Google Cloud Storage
Extracts data from MongoDB collections and uploads as CSV files to GCS bucket using Dataflow
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
import pandas as pd
import pymongo
from pymongo import MongoClient
import io
import csv
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any
import argparse
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBReadTransform(beam.DoFn):
    """
    Custom DoFn to read data from MongoDB collections
    """
    
    def __init__(self, mongo_uri: str, database_name: str, collection_name: str):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        
    def setup(self):
        """Initialize MongoDB connection per worker"""
        self.client = MongoClient(self.mongo_uri)
        logger.info(f"Connected to MongoDB for collection: {self.collection_name}")
        
    def process(self, element):
        """
        Process method that reads from MongoDB collection
        element is ignored, this transform generates its own data
        """
        try:
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            # Read all documents from collection
            cursor = collection.find({})
            
            for document in cursor:
                # Convert ObjectId to string for JSON serialization
                if '_id' in document:
                    document['_id'] = str(document['_id'])
                
                yield document
                
        except Exception as e:
            logger.error(f"Error reading from MongoDB collection {self.collection_name}: {str(e)}")
            raise
    
    def teardown(self):
        """Clean up MongoDB connection"""
        if self.client:
            self.client.close()

class ConvertToCSVTransform(beam.DoFn):
    """
    Transform that converts MongoDB documents to CSV format
    """
    
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.headers_written = False
        self.csv_headers = None
        
    def process(self, element):
        """Convert MongoDB document to CSV row"""
        try:
            # Convert document to flat dictionary
            flat_doc = self._flatten_document(element)
            
            # Generate headers from first document
            if self.csv_headers is None:
                self.csv_headers = list(flat_doc.keys())
                # Yield header row first
                yield ','.join(self.csv_headers)
            
            # Create CSV row
            csv_row = []
            for header in self.csv_headers:
                value = flat_doc.get(header, '')
                # Handle None values and escape quotes
                if value is None:
                    value = ''
                elif isinstance(value, str):
                    value = value.replace('"', '""')  # Escape quotes
                csv_row.append(f'"{value}"')
            
            yield ','.join(csv_row)
            
        except Exception as e:
            logger.error(f"Error converting document to CSV: {str(e)}")
            raise
    
    def _flatten_document(self, doc: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
        """
        Flatten nested MongoDB document for CSV conversion
        """
        flattened = {}
        
        for key, value in doc.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                flattened.update(self._flatten_document(value, new_key))
            elif isinstance(value, list):
                # Convert lists to string representation
                flattened[new_key] = json.dumps(value) if value else ''
            else:
                # Handle primitive types
                flattened[new_key] = str(value) if value is not None else ''
        
        return flattened

def run_pipeline(argv=None):
    """
    Main pipeline execution function
    """
    parser = argparse.ArgumentParser()
    
    # MongoDB configuration
    parser.add_argument(
        '--mongo_uri',
        default=os.getenv('MONGO_URI'),
        help='MongoDB connection URI (or set MONGO_URI environment variable)'
    )
    parser.add_argument(
        '--mongo_database',
        default=os.getenv('MONGO_DATABASE', 'brazilian-ecommerce'),
        help='MongoDB database name (or set MONGO_DATABASE environment variable)'
    )
    
    # Google Cloud configuration
    parser.add_argument(
        '--project',
        required=True,
        help='Google Cloud Project ID'
    )
    parser.add_argument(
        '--gcs_bucket',
        required=True,
        help='Google Cloud Storage bucket name (without gs:// prefix)'
    )
    parser.add_argument(
        '--gcs_prefix',
        default='brazilian-ecommerce-exports',
        help='GCS prefix for exported files'
    )
    parser.add_argument(
        '--temp_location',
        required=True,
        help='GCS temp location for Dataflow (e.g., gs://bucket/temp)'
    )
    parser.add_argument(
        '--staging_location',
        required=True,
        help='GCS staging location for Dataflow (e.g., gs://bucket/staging)'
    )
    
    # Collections to export
    parser.add_argument(
        '--collections',
        default='customers,orders,order_items,order_payments,order_reviews,products,sellers,geolocation,product_categories',
        help='Comma-separated list of collections to export'
    )
    
    # Pipeline options
    parser.add_argument(
        '--runner',
        default='DataflowRunner',
        help='Pipeline runner (DirectRunner for local, DataflowRunner for cloud)'
    )
    parser.add_argument(
        '--region',
        default='us-central1',
        help='Google Cloud region for Dataflow'
    )
    parser.add_argument(
        '--machine_type',
        default='n1-standard-1',
        help='Machine type for Dataflow workers'
    )
    parser.add_argument(
        '--max_num_workers',
        type=int,
        default=5,
        help='Maximum number of Dataflow workers'
    )
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Configure pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Google Cloud options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.temp_location = known_args.temp_location
    google_cloud_options.staging_location = known_args.staging_location
    google_cloud_options.region = known_args.region
    
    # Standard options
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.runner = known_args.runner
    
    # Worker options
    worker_options = pipeline_options.view_as(WorkerOptions)
    worker_options.machine_type = known_args.machine_type
    worker_options.max_num_workers = known_args.max_num_workers
    
    # Parse collections list
    collections = [col.strip() for col in known_args.collections.split(',')]
    
    logger.info(f"Starting pipeline with runner: {known_args.runner}")
    logger.info(f"Collections to export: {collections}")
    logger.info(f"Output bucket: gs://{known_args.gcs_bucket}/{known_args.gcs_prefix}")
    
    # Create and run pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        for collection_name in collections:
            logger.info(f"Creating pipeline branch for collection: {collection_name}")
            
            # Create pipeline branch for each collection
            (pipeline
             | f'Create_{collection_name}' >> beam.Create([None])  # Dummy element to trigger MongoDB read
             | f'Read_MongoDB_{collection_name}' >> beam.ParDo(
                 MongoDBReadTransform(
                     known_args.mongo_uri,
                     known_args.mongo_database,
                     collection_name
                 )
             )
             | f'Convert_to_CSV_{collection_name}' >> beam.ParDo(
                 ConvertToCSVTransform(collection_name)
             )
             | f'Write_to_GCS_{collection_name}' >> beam.io.WriteToText(
                 f'gs://{known_args.gcs_bucket}/{known_args.gcs_prefix}/{collection_name}',
                 file_name_suffix='.csv',
                 num_shards=1  # Single file output
             )
            )
    
    logger.info("Pipeline execution completed!")

def create_dataflow_job_script():
    """
    Create a helper script to run the Dataflow job
    """
    script_content = '''#!/bin/bash
# Dataflow Job Execution Script
# Update the variables below with your specific configuration

# Google Cloud Configuration
export PROJECT_ID="your-gcp-project-id"
export GCS_BUCKET="your-gcs-bucket-name"
export REGION="us-central1"

# Create GCS bucket if it doesn't exist
gsutil mb -p $PROJECT_ID gs://$GCS_BUCKET 2>/dev/null || echo "Bucket already exists"

# Run the Dataflow pipeline
python mongodb_to_gcs_pipeline.py \\
    --project=$PROJECT_ID \\
    --gcs_bucket=$GCS_BUCKET \\
    --temp_location=gs://$GCS_BUCKET/temp \\
    --staging_location=gs://$GCS_BUCKET/staging \\
    --runner=DataflowRunner \\
    --region=$REGION \\
    --machine_type=n1-standard-2 \\
    --max_num_workers=10 \\
    --collections=customers,orders,order_items,order_payments,order_reviews,products,sellers,geolocation,product_categories

echo "Dataflow job submitted. Check Google Cloud Console for progress."
'''
    
    with open('run_dataflow_job.sh', 'w') as f:
        f.write(script_content)
    
    # Make script executable
    import os
    os.chmod('run_dataflow_job.sh', 0o755)
    
    logger.info("Created run_dataflow_job.sh script")

def test_local_pipeline():
    """
    Test the pipeline locally with DirectRunner
    """
    test_args = [
        '--project=test-project',
        '--gcs_bucket=test-bucket',
        '--temp_location=gs://test-bucket/temp',
        '--staging_location=gs://test-bucket/staging',
        '--runner=DirectRunner',
        '--collections=customers',  # Test with just one collection
    ]
    
    logger.info("Running local test pipeline...")
    try:
        run_pipeline(test_args)
        logger.info("Local test completed successfully!")
    except Exception as e:
        logger.error(f"Local test failed: {str(e)}")
        return False
    
    return True

if __name__ == '__main__':
    import sys
    
    # Check if this is a test run
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        test_local_pipeline()
    elif len(sys.argv) > 1 and sys.argv[1] == '--create-script':
        create_dataflow_job_script()
    else:
        run_pipeline()
