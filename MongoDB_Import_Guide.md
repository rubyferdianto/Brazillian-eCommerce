# MongoDB Import Guide for Brazilian E-commerce Dataset

This guide helps you import your CSV files into MongoDB for better querying and analysis.

## Prerequisites

### 1. Install MongoDB
**macOS (using Homebrew):**
```bash
# Install MongoDB
brew tap mongodb/brew
brew install mongodb-community

# Start MongoDB service
brew services start mongodb/brew/mongodb-community
```

**Alternative - MongoDB Atlas (Cloud):**
- Sign up at https://cloud.mongodb.com
- Create a free cluster
- Get your connection string

### 2. Install Python Dependencies
```bash
# Install required Python packages
pip install -r requirements.txt

# Or install individually
pip install pymongo pandas numpy
```

## Quick Start

### 1. Run the Import Script
```bash
# Make sure MongoDB is running, then:
python import_to_mongodb.py
```

### 2. Verify Import
```bash
# Connect to MongoDB
mongosh brazilian_ecommerce

# List collections
show collections

# View sample data
db.orders.find().limit(3)
```

## Configuration Options

### Custom MongoDB Connection
Edit the script to change connection settings:

```python
# In import_to_mongodb.py, modify these variables:
MONGODB_URI = "mongodb://localhost:27017/"  # Local MongoDB
# Or for MongoDB Atlas:
# MONGODB_URI = "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/"

DATABASE_NAME = "brazilian_ecommerce"  # Your database name
DATA_DIRECTORY = "data"  # Directory containing CSV files
```

## What Gets Imported

The script imports these CSV files into MongoDB collections:

| CSV File | MongoDB Collection | Description |
|----------|-------------------|-------------|
| `olist_customers_dataset.csv` | `customers` | Customer information |
| `olist_orders_dataset.csv` | `orders` | Order details |
| `olist_order_items_dataset.csv` | `order_items` | Items in each order |
| `olist_order_payments_dataset.csv` | `order_payments` | Payment information |
| `olist_order_reviews_dataset.csv` | `order_reviews` | Customer reviews |
| `olist_products_dataset.csv` | `products` | Product catalog |
| `olist_sellers_dataset.csv` | `sellers` | Seller information |
| `olist_geolocation_dataset.csv` | `geolocation` | Geographic data |
| `product_category_name_translation.csv` | `product_categories` | Category translations |

## Key Features

### ✅ **Automatic Indexing**
The script creates indexes on key fields:
- `order_id` - for fast order lookups
- `customer_id` - for customer queries
- `product_id` - for product searches
- `seller_id` - for seller analysis

### ✅ **Data Cleaning**
- Converts pandas NaN values to MongoDB null
- Handles data type conversions automatically
- Preserves original data structure

### ✅ **Metadata Collection**
Creates a `metadata` collection with:
- Import timestamp
- Collection statistics
- Sample documents
- Database information

## MongoDB Queries

### Basic Queries
```javascript
// Find orders from a specific customer
db.orders.find({"customer_id": "customer_123"})

// Find high-value orders
db.order_items.find({"price": {$gt: 100}})

// Count total customers
db.customers.countDocuments()
```

### Aggregation Examples
```javascript
// Top 10 products by sales
db.order_items.aggregate([
  {$group: {_id: "$product_id", total_sales: {$sum: "$price"}}},
  {$sort: {total_sales: -1}},
  {$limit: 10}
])

// Orders by state
db.customers.aggregate([
  {$group: {_id: "$customer_state", count: {$sum: 1}}},
  {$sort: {count: -1}}
])
```

### Join Collections
```javascript
// Join orders with customer information
db.orders.aggregate([
  {
    $lookup: {
      from: "customers",
      localField: "customer_id",
      foreignField: "customer_id",
      as: "customer_info"
    }
  }
])
```

## Benefits of MongoDB vs CSV

### 🚀 **Performance**
- **Indexed queries**: Fast lookups on ID fields
- **Aggregation pipeline**: Complex analytics without loading all data
- **Memory efficiency**: Query only needed fields

### 🔍 **Querying Power**
- **Complex filters**: Multiple conditions, ranges, regex
- **Joins**: Connect related collections easily
- **Geospatial queries**: Location-based analysis with geolocation data

### 📊 **Analytics**
- **Real-time aggregations**: Calculate metrics on-demand
- **Time-series analysis**: Query by date ranges efficiently
- **Flexible schema**: Add new fields without restructuring

## Troubleshooting

### MongoDB Connection Issues
```bash
# Check if MongoDB is running
brew services list | grep mongodb

# Start MongoDB
brew services start mongodb/brew/mongodb-community

# Check MongoDB logs
tail -f /usr/local/var/log/mongodb/mongo.log
```

### Import Errors
```bash
# Check CSV files exist
ls -la data/

# Run with verbose logging
python import_to_mongodb.py 2>&1 | tee import.log
```

### Memory Issues (Large Files)
If you have memory issues with large CSV files, modify the script to use chunked imports:

```python
# Read CSV in chunks
chunksize = 10000
for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
    records = chunk.to_dict('records')
    collection.insert_many(records)
```

## Next Steps

After importing, you can:

1. **Connect from Python:**
   ```python
   from pymongo import MongoClient
   client = MongoClient("mongodb://localhost:27017/")
   db = client.brazilian_ecommerce
   orders = db.orders.find({"order_status": "delivered"})
   ```

2. **Use MongoDB Compass** (GUI):
   - Download from https://www.mongodb.com/products/compass
   - Connect to `mongodb://localhost:27017`
   - Browse collections visually

3. **Build APIs:**
   - Use FastAPI or Flask with PyMongo
   - Create REST endpoints for your data

4. **Business Intelligence:**
   - Connect to tools like Tableau or Power BI
   - Use MongoDB BI Connector

Happy analyzing! 🎉
