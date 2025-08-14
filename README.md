# Brazilian E-Commerce Data Pipeline

This repository contains a Google Cloud Function solution for exporting Brazilian e-commerce data from MongoDB to Google Cloud Storage as CSV files.

## 🏗️ Architecture

**Current Solution: Google Cloud Function**
- **Function Name**: `mongodb-csv-exporter`
- **Runtime**: Python 3.11
- **Trigger**: HTTP POST
- **Memory**: 1GB
- **Timeout**: 540 seconds

## 📁 Repository Structure

```
├── cloud_function/          # Google Cloud Function implementation
│   ├── main.py             # Main function code
│   └── requirements.txt    # Python dependencies
├── data/                   # Original CSV datasets
├── simple_csv_export.py    # Local MongoDB export script
├── deploy.sh              # Cloud Function deployment script
├── import.ipynb           # Jupyter notebook for data analysis
├── BE-import.ipynb        # Brazilian e-commerce import notebook
├── .env.example           # Environment variables template
└── Documentation files
```

## 🚀 Quick Start

### 1. Deploy the Cloud Function

```bash
./deploy.sh
```

### 2. Trigger the Export

```bash
curl -X POST "https://us-central1-infinite-byte-458600-a8.cloudfunctions.net/mongodb-csv-exporter" \
     -H "Content-Type: application/json" \
     -d '{}'
```

## ⚙️ Configuration

The function uses these environment variables:

- `MONGO_URI`: MongoDB connection string
- `DATABASE_NAME`: Target database name ("brazilian-ecommerce")
- `BUCKET_NAME`: Google Cloud Storage bucket ("my_bec_bucket")

## 📊 Data Collections

The pipeline exports these collections from MongoDB:

- `customers` - Customer information
- `sellers` - Seller details
- `orders` - Order records
- `order_items` - Order line items
- `order_payments` - Payment information
- `order_reviews` - Customer reviews
- `products` - Product catalog
- `geolocation` - Geographic data
- `product_categories` - Category translations

## 🔧 Local Development

For local testing, use the `simple_csv_export.py` script:

```bash
python simple_csv_export.py
```

## 📚 Documentation

- `SECURITY_SETUP.md` - Security configuration guide
- `MongoDB_Import_Guide.md` - MongoDB setup instructions
- `CLEANUP_SUMMARY.md` - Repository cleanup history

## 🏷️ Version History

- **v2.0** - Google Cloud Function implementation (current)
- **v1.0** - Apache Beam/Dataflow approach (deprecated)

---

**Status**: ✅ **Production Ready**
**Last Updated**: August 14, 2025
