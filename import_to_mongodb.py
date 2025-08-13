#!/usr/bin/env python3
"""
MongoDB Import Script for Brazilian E-commerce Dataset
This script imports all CSV files from the data directory into MongoDB
"""

import pandas as pd
import pymongo
from pymongo import MongoClient
import os
from pathlib import Path
import json
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBImporter:
    def __init__(self, connection_string=None, database_name=None):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string (str): MongoDB connection string (optional, uses env var if not provided)
            database_name (str): Name of the database to create/use (optional, uses env var if not provided)
        """
        # Use environment variables if not provided
        self.connection_string = connection_string or os.getenv('MONGO_URI')
        self.database_name = database_name or os.getenv('MONGO_DATABASE', 'brazilian-ecommerce')
        
        if not self.connection_string:
            raise ValueError("MongoDB connection string not provided. Set MONGO_URI environment variable or pass connection_string parameter.")
        
        self.client = None
        self.db = None
        
    def connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            logger.info(f"Successfully connected to MongoDB: {self.database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            return False
    
    def import_csv_to_collection(self, csv_file_path, collection_name):
        """
        Import a CSV file to a MongoDB collection
        
        Args:
            csv_file_path (str): Path to the CSV file
            collection_name (str): Name of the collection to create
        """
        try:
            # Read CSV file
            logger.info(f"Reading CSV file: {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Handle NaN values (MongoDB doesn't support NaN)
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            # Get collection
            collection = self.db[collection_name]
            
            # Drop existing collection if it exists
            if collection_name in self.db.list_collection_names():
                collection.drop()
                logger.info(f"Dropped existing collection: {collection_name}")
            
            # Insert records
            if records:
                result = collection.insert_many(records)
                logger.info(f"Successfully imported {len(result.inserted_ids)} records to {collection_name}")
                
                # Create index on common ID fields
                if 'order_id' in df.columns:
                    collection.create_index('order_id')
                    logger.info(f"Created index on order_id for {collection_name}")
                if 'customer_id' in df.columns:
                    collection.create_index('customer_id')
                    logger.info(f"Created index on customer_id for {collection_name}")
                if 'product_id' in df.columns:
                    collection.create_index('product_id')
                    logger.info(f"Created index on product_id for {collection_name}")
                if 'seller_id' in df.columns:
                    collection.create_index('seller_id')
                    logger.info(f"Created index on seller_id for {collection_name}")
                    
            return True
            
        except Exception as e:
            logger.error(f"Failed to import {csv_file_path}: {str(e)}")
            return False
    
    def import_all_csv_files(self, data_directory="data"):
        """
        Import all CSV files from the data directory
        
        Args:
            data_directory (str): Directory containing CSV files
        """
        data_dir = Path(data_directory)
        
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_directory}")
            return False
        
        # Define CSV files and their collection names
        csv_files = {
            'olist_customers_dataset.csv': 'customers',
            'olist_geolocation_dataset.csv': 'geolocation',
            'olist_order_items_dataset.csv': 'order_items',
            'olist_order_payments_dataset.csv': 'order_payments',
            'olist_order_reviews_dataset.csv': 'order_reviews',
            'olist_orders_dataset.csv': 'orders',
            'olist_products_dataset.csv': 'products',
            'olist_sellers_dataset.csv': 'sellers',
            'product_category_name_translation.csv': 'product_categories'
        }
        
        success_count = 0
        total_files = len(csv_files)
        
        logger.info(f"Starting import of {total_files} CSV files...")
        
        for filename, collection_name in csv_files.items():
            file_path = data_dir / filename
            
            if file_path.exists():
                if self.import_csv_to_collection(file_path, collection_name):
                    success_count += 1
                else:
                    logger.error(f"Failed to import {filename}")
            else:
                logger.warning(f"File not found: {filename}")
        
        logger.info(f"Import completed: {success_count}/{total_files} files imported successfully")
        return success_count == total_files
    
    def create_database_info(self):
        """Create a metadata collection with database information"""
        try:
            metadata = {
                'database_name': self.database_name,
                'import_date': datetime.now(),
                'source': 'Brazilian E-commerce Dataset (Olist)',
                'description': 'Complete Brazilian e-commerce dataset with orders, customers, products, sellers, payments, and reviews',
                'collections': list(self.db.list_collection_names()),
                'total_collections': len(list(self.db.list_collection_names()))
            }
            
            # Add collection statistics
            collection_stats = {}
            for collection_name in self.db.list_collection_names():
                if collection_name != 'metadata':
                    collection = self.db[collection_name]
                    count = collection.count_documents({})
                    collection_stats[collection_name] = {
                        'document_count': count,
                        'sample_document': collection.find_one() if count > 0 else None
                    }
            
            metadata['collection_statistics'] = collection_stats
            
            # Insert metadata
            metadata_collection = self.db['metadata']
            metadata_collection.drop()  # Remove existing metadata
            metadata_collection.insert_one(metadata)
            
            logger.info("Database metadata created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database metadata: {str(e)}")
            return False
    
    def print_import_summary(self):
        """Print a summary of the imported data"""
        try:
            print("\n" + "="*60)
            print("MONGODB IMPORT SUMMARY")
            print("="*60)
            print(f"Database: {self.database_name}")
            print(f"Connection: {self.connection_string}")
            print(f"Collections imported: {len(list(self.db.list_collection_names())) - 1}")  # -1 for metadata
            print("\nCollection Details:")
            print("-" * 60)
            
            total_documents = 0
            for collection_name in sorted(self.db.list_collection_names()):
                if collection_name != 'metadata':
                    collection = self.db[collection_name]
                    count = collection.count_documents({})
                    total_documents += count
                    print(f"📊 {collection_name:<25} | {count:>10,} documents")
            
            print("-" * 60)
            print(f"{'TOTAL':<25} | {total_documents:>10,} documents")
            print("\n✅ Import completed successfully!")
            print("\n💡 Usage Examples:")
            print("   # Connect to database")
            print(f"   mongo {self.database_name}")
            print("   # View collections")
            print("   show collections")
            print("   # Query orders")
            print("   db.orders.find().limit(5)")
            
        except Exception as e:
            logger.error(f"Failed to print summary: {str(e)}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function to run the import process"""
    
    # Configuration
    MONGODB_URI = "mongodb+srv://rubyferdianto:lm6xwg6OJKjH6UwS@cluster0.thisg0i.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"  # Change this if your MongoDB is elsewhere
    DATABASE_NAME = "brazilian-ecommerce"
    DATA_DIRECTORY = "data"
    
    print("🚀 Brazilian E-commerce CSV to MongoDB Importer")
    print("=" * 50)
    
    # Create importer instance
    importer = MongoDBImporter(MONGODB_URI, DATABASE_NAME)
    
    # Connect to MongoDB
    if not importer.connect():
        print("❌ Failed to connect to MongoDB. Please ensure MongoDB is running.")
        print("💡 Install MongoDB: https://docs.mongodb.com/manual/installation/")
        return
    
    # Import all CSV files
    if importer.import_all_csv_files(DATA_DIRECTORY):
        # Create database metadata
        importer.create_database_info()
        
        # Print summary
        importer.print_import_summary()
    else:
        print("❌ Import process failed. Check the logs above for details.")
    
    # Close connection
    importer.close_connection()

if __name__ == "__main__":
    main()
