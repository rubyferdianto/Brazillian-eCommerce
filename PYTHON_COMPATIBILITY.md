# Python Version Compatibility Guide

## 🐍 Apache Beam Compatibility Issue

You're encountering this error because **Apache Beam doesn't fully support Python 3.13 yet**. Apache Beam currently supports Python 3.8 through 3.11.

## 🔧 Solutions

### Option 1: Use Basic Requirements (Recommended for Python 3.12+)
```bash
# Install basic dependencies without Apache Beam
pip install -r requirements-basic.txt
```

Then use the simple export script:
```bash
# Export using simple method (without Apache Beam/Dataflow)
python simple_mongo_to_gcs.py
```

### Option 2: Use Python 3.11 Environment
```bash
# Create a new conda environment with Python 3.11
conda create -n beam-env python=3.11
conda activate beam-env

# Install full requirements with Apache Beam
pip install -r requirements.txt
```

### Option 3: Use pyenv to Install Python 3.11
```bash
# Install Python 3.11 using pyenv
pyenv install 3.11.9
pyenv local 3.11.9

# Create virtual environment
python -m venv beam-venv
source beam-venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

## 📋 What Each Solution Provides

### Basic Version (Python 3.12+)
✅ **Available Features:**
- MongoDB data import
- CSV export to local files
- Direct upload to Google Cloud Storage
- Data analysis and visualization
- Environment variable management

❌ **Not Available:**
- Apache Beam pipeline
- Google Cloud Dataflow execution
- Distributed processing

### Full Version (Python 3.8-3.11)
✅ **All Features Available:**
- Everything from basic version
- Apache Beam pipeline development
- Google Cloud Dataflow deployment
- Distributed, scalable processing
- Advanced data transformations

## 🚀 Quick Start for Your Current Setup (Python 3.13)

Since you have Python 3.13, here's the working solution:

### 1. Install Minimal Dependencies
```bash
pip install -r requirements-minimal.txt
```

### 2. Set Up Environment
```bash
# Copy environment template (if not done already)
cp .env.example .env

# Edit with your credentials
nano .env
```

### 3. Test MongoDB Connection
```bash
# List available collections
python simple_csv_export.py --list

# Test with one collection
python simple_csv_export.py --collections customers --limit 100
```

### 4. Export All Data to CSV
```bash
# Export all collections
python simple_csv_export.py

# Export specific collections
python simple_csv_export.py --collections customers,orders,products

# Export to custom directory
python simple_csv_export.py --output-dir my-exports
```

### 5. Upload to Google Cloud Storage (Optional)
```bash
# If you have Google Cloud SDK installed
gsutil -m cp exports/*.csv gs://your-bucket-name/brazilian-ecommerce/
```

## 📊 Performance Comparison

| Method | Python Version | Processing Speed | Scalability | Setup Complexity |
|--------|---------------|------------------|-------------|------------------|
| Simple Export | 3.8+ | Fast for <1M records | Single machine | Low |
| Apache Beam | 3.8-3.11 | Very fast | Distributed | Medium |
| Dataflow | 3.8-3.11 | Fastest | Auto-scaling | High |

## 💡 Recommendations

**For your current setup (Python 3.13):**
- Use `requirements-basic.txt` and `simple_mongo_to_gcs.py`
- This will handle your ~1.8M records efficiently
- Setup is much simpler and works immediately

**For production/large-scale processing:**
- Consider using Python 3.11 environment
- Apache Beam provides better error handling and scalability
- Dataflow offers managed, auto-scaling infrastructure

## 🔄 Migration Path

If you start with the simple version and later want Apache Beam:

1. Create Python 3.11 environment
2. Install full requirements
3. Your data and configuration will work with both approaches

---

**Choose the option that best fits your current needs and Python version!**
