#!/usr/bin/env python3
"""
Simple MongoDB Import Script
Step-by-step CSV import with user prompts
"""

import pandas as pd
import os
from pathlib import Path

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("🔍 Checking prerequisites...")
    
    # Check if data directory exists
    if not Path("data").exists():
        print("❌ 'data' directory not found!")
        print("💡 Please ensure your CSV files are in a 'data' folder")
        return False
    
    # Check for CSV files
    csv_files = [
        'olist_customers_dataset.csv',
        'olist_orders_dataset.csv',
        'olist_order_items_dataset.csv',
        'olist_order_payments_dataset.csv',
        'olist_order_reviews_dataset.csv',
        'olist_products_dataset.csv',
        'olist_sellers_dataset.csv',
        'olist_geolocation_dataset.csv',
        'product_category_name_translation.csv'
    ]
    
    missing_files = []
    for file in csv_files:
        if not Path(f"data/{file}").exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing CSV files: {', '.join(missing_files)}")
        return False
    
    print("✅ All CSV files found!")
    
    # Check if pymongo is installed
    try:
        import pymongo
        print("✅ PyMongo is installed")
    except ImportError:
        print("❌ PyMongo not installed!")
        print("💡 Install with: pip install pymongo")
        return False
    
    return True

def test_mongodb_connection():
    """Test MongoDB connection"""
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ MongoDB connection successful!")
        client.close()
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {str(e)}")
        print("💡 Make sure MongoDB is running:")
        print("   brew services start mongodb/brew/mongodb-community")
        return False

def import_single_csv(csv_file, collection_name, db):
    """Import a single CSV file to MongoDB"""
    try:
        print(f"📁 Importing {csv_file}...")
        
        # Read CSV
        df = pd.read_csv(f"data/{csv_file}")
        print(f"   📊 Found {len(df):,} records")
        
        # Convert to dict and handle NaN values
        records = df.to_dict('records')
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
        
        # Import to MongoDB
        collection = db[collection_name]
        
        # Ask user if they want to replace existing data
        if collection_name in db.list_collection_names():
            choice = input(f"   ⚠️  Collection '{collection_name}' exists. Replace? (y/n): ")
            if choice.lower() == 'y':
                collection.drop()
                print(f"   🗑️  Dropped existing collection")
        
        # Insert data
        if records:
            result = collection.insert_many(records)
            print(f"   ✅ Imported {len(result.inserted_ids):,} records to '{collection_name}'")
            
            # Create basic indexes
            if 'order_id' in df.columns:
                collection.create_index('order_id')
                print(f"   🔍 Created index on order_id")
            if 'customer_id' in df.columns:
                collection.create_index('customer_id')
                print(f"   🔍 Created index on customer_id")
            if 'product_id' in df.columns:
                collection.create_index('product_id')
                print(f"   🔍 Created index on product_id")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to import {csv_file}: {str(e)}")
        return False

def main():
    """Main interactive import process"""
    print("🚀 MongoDB CSV Importer - Brazilian E-commerce Dataset")
    print("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please fix the issues above.")
        return
    
    # Test MongoDB connection
    if not test_mongodb_connection():
        print("\n❌ Cannot connect to MongoDB. Please start MongoDB first.")
        return
    
    # Get database name
    db_name = input("\n📝 Enter database name (default: brazilian_ecommerce): ").strip()
    if not db_name:
        db_name = "brazilian_ecommerce"
    
    # Connect to MongoDB
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    
    print(f"\n📊 Connected to database: {db_name}")
    
    # Define files to import
    files_to_import = [
        ('olist_customers_dataset.csv', 'customers'),
        ('olist_orders_dataset.csv', 'orders'),
        ('olist_order_items_dataset.csv', 'order_items'),
        ('olist_order_payments_dataset.csv', 'order_payments'),
        ('olist_order_reviews_dataset.csv', 'order_reviews'),
        ('olist_products_dataset.csv', 'products'),
        ('olist_sellers_dataset.csv', 'sellers'),
        ('olist_geolocation_dataset.csv', 'geolocation'),
        ('product_category_name_translation.csv', 'product_categories')
    ]
    
    # Ask user which files to import
    print(f"\n📋 Found {len(files_to_import)} CSV files to import:")
    for i, (csv_file, collection_name) in enumerate(files_to_import, 1):
        print(f"   {i}. {csv_file} → {collection_name}")
    
    choice = input(f"\nImport all files? (y/n): ").strip().lower()
    
    if choice == 'y':
        print("\n🔄 Starting bulk import...")
        success_count = 0
        
        for csv_file, collection_name in files_to_import:
            if import_single_csv(csv_file, collection_name, db):
                success_count += 1
            print()  # Empty line for readability
        
        print(f"✅ Import completed! {success_count}/{len(files_to_import)} files imported successfully")
        
    else:
        print("\n🔄 Interactive import mode:")
        for csv_file, collection_name in files_to_import:
            choice = input(f"Import {csv_file}? (y/n/q to quit): ").strip().lower()
            if choice == 'q':
                break
            elif choice == 'y':
                import_single_csv(csv_file, collection_name, db)
            print()
    
    # Show final summary
    print("\n" + "="*60)
    print("📊 IMPORT SUMMARY")
    print("="*60)
    print(f"Database: {db_name}")
    
    collections = db.list_collection_names()
    total_docs = 0
    
    for collection_name in sorted(collections):
        count = db[collection_name].count_documents({})
        total_docs += count
        print(f"📁 {collection_name:<20} | {count:>8,} documents")
    
    print("-" * 40)
    print(f"📊 TOTAL: {total_docs:,} documents in {len(collections)} collections")
    
    print(f"\n💡 Next steps:")
    print(f"   # Connect to your database")
    print(f"   mongosh {db_name}")
    print(f"   # View collections")
    print(f"   show collections")
    print(f"   # Query data")
    print(f"   db.orders.find().limit(5)")
    
    client.close()
    print(f"\n🎉 All done! Your data is now in MongoDB.")

if __name__ == "__main__":
    main()
