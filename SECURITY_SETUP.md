# Security Setup Guide

## 🔐 Environment Variables Configuration

This project now uses environment variables to securely manage sensitive credentials like MongoDB connection strings and Google Cloud project information.

## 📋 Setup Steps

### 1. Copy Environment Template
```bash
cp .env.example .env
```

### 2. Edit Your Environment File
```bash
nano .env
```

Update the following variables with your actual values:

```bash
# MongoDB Configuration
MONGO_URI=mongodb+srv://rubyferdianto:lm6xwg6OJKjH6UwS@cluster0.thisg0i.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
MONGO_DATABASE=brazilian-ecommerce

# Google Cloud Configuration
GCP_PROJECT_ID=infinite-byte-458600-a8
GCS_BUCKET=my_bec_bucket
GCP_REGION=us-central1

# Dataflow Configuration
DATAFLOW_MACHINE_TYPE=n1-standard-2
DATAFLOW_MAX_WORKERS=10
```

### 3. Install Dependencies
```bash
# Install all dependencies (MongoDB + Apache Beam + GCP)
pip install -r requirements.txt
```

## 🛡️ Security Features

### Environment File Protection
- **`.env`** - Contains your actual credentials (NEVER commit to git)
- **`.env.example`** - Template file (safe to commit)
- **`.gitignore`** - Automatically excludes `.env` files

### What's Protected
- ✅ MongoDB connection strings with credentials
- ✅ Google Cloud project IDs
- ✅ GCS bucket names
- ✅ All sensitive configuration

### Git Safety
The `.gitignore` file now includes:
```gitignore
# Environment Files - IMPORTANT: Contains sensitive credentials
.env
.env.local
.env.*.local
*.env
```

## 🔧 Usage After Setup

### MongoDB Import Script
```bash
# No code changes needed - automatically uses .env
python import_to_mongodb.py
```

### Pipeline Testing
```bash
# Automatically loads from .env
python test_pipeline.py
```

### Dataflow Execution
```bash
# Loads configuration from .env
./run_dataflow_job.sh
```

## ⚠️ Important Security Notes

### DO NOT:
- ❌ Commit `.env` file to git
- ❌ Share `.env` file in chat/email
- ❌ Include credentials in code comments
- ❌ Push hardcoded credentials to repositories

### DO:
- ✅ Use `.env.example` as template
- ✅ Keep `.env` file local only
- ✅ Rotate credentials regularly
- ✅ Use different credentials for different environments

## 🔄 Migration from Hardcoded Credentials

If you previously had hardcoded credentials:

### 1. Remove from Git History (if needed)
```bash
# Remove sensitive files from git history
git filter-branch --force --index-filter \
'git rm --cached --ignore-unmatch .env' \
--prune-empty --tag-name-filter cat -- --all

# Force push (be careful!)
git push origin --force --all
```

### 2. Verify Clean State
```bash
# Check that credentials are not in any files
grep -r "mongodb+srv" . --exclude-dir=.git --exclude="*.md"
grep -r "your-actual-password" . --exclude-dir=.git
```

## 🚨 Emergency Procedures

### If Credentials Are Compromised:
1. **Immediately rotate MongoDB credentials** in Atlas
2. **Update `.env` file** with new credentials
3. **Remove old credentials** from any git history
4. **Review access logs** in MongoDB Atlas

### If `.env` Is Accidentally Committed:
1. **Immediately remove** from repository
2. **Force push** to remove from history
3. **Rotate all credentials** in the file
4. **Update `.env`** with new credentials

## 📞 Support

For security-related questions:
- MongoDB Atlas Security: [Documentation](https://docs.atlas.mongodb.com/security/)
- Google Cloud Security: [Best Practices](https://cloud.google.com/security/best-practices)
- Git Security: [Removing Sensitive Data](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository)

---

**Remember: Security is everyone's responsibility. Keep your credentials safe!** 🔒
