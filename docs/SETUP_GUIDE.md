# Data Quality Framework - Comprehensive Setup Guide

This guide provides step-by-step instructions to set up the complete data quality framework environment, including PostgreSQL, Apache Superset, and all dependencies.

## 📋 Table of Contents

1. [System Requirements](#system-requirements)
2. [PostgreSQL Setup](#postgresql-setup)
3. [Python Environment Setup](#python-environment-setup)
4. [Framework Installation](#framework-installation)
5. [Configuration](#configuration)
6. [Apache Superset Setup](#apache-superset-setup)
7. [First Run and Testing](#first-run-and-testing)
8. [Dashboard Creation](#dashboard-creation)
9. [Troubleshooting](#troubleshooting)
10. [Maintenance](#maintenance)

---

## 1. System Requirements

### Minimum Requirements
- **Operating System**: Windows 10/11, macOS 10.15+, or Linux (Ubuntu 18.04+)
- **RAM**: 8GB minimum, 16GB recommended
- **Storage**: 10GB free space
- **Python**: 3.8 or higher
- **PostgreSQL**: 12 or higher

### Recommended Software
- **Git**: For version control
- **VS Code**: For code editing
- **pgAdmin**: For PostgreSQL management
- **Docker** (optional): For containerized setup

---

## 2. PostgreSQL Setup

### Option A: Native Installation (Recommended)

#### Windows
1. **Download PostgreSQL**:
   - Go to https://www.postgresql.org/download/windows/
   - Download PostgreSQL 15 or later
   - Run the installer as Administrator

2. **Installation Settings**:
   ```
   Port: 5432 (default)
   Superuser: postgres
   Password: [choose a strong password]
   ```

3. **Verify Installation**:
   ```cmd
   psql --version
   ```

#### macOS
```bash
# Using Homebrew
brew install postgresql@15
brew services start postgresql@15

# Or using MacPorts
sudo port install postgresql15-server
sudo port load postgresql15-server
```

#### Linux (Ubuntu/Debian)
```bash
# Update package list
sudo apt update

# Install PostgreSQL
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### Option B: Docker Setup (Alternative)

```bash
# Pull PostgreSQL image
docker pull postgres:15

# Run PostgreSQL container
docker run --name data-quality-postgres \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=data_quality_db \
  -p 5432:5432 \
  -d postgres:15
```

### Database Setup

1. **Connect to PostgreSQL**:
   ```bash
   psql -U postgres -h localhost -p 5432
   ```

2. **Create Database**:
   ```sql
   CREATE DATABASE data_quality_db;
   CREATE USER dq_user WITH PASSWORD 'dq_password';
   GRANT ALL PRIVILEGES ON DATABASE data_quality_db TO dq_user;
   \q
   ```

3. **Test Connection**:
   ```bash
   psql -U dq_user -h localhost -p 5432 -d data_quality_db
   ```

---

## 3. Python Environment Setup

### Install Python (if not already installed)

#### Windows
1. Download from https://www.python.org/downloads/
2. **Important**: Check "Add Python to PATH" during installation
3. Verify: `python --version`

#### macOS
```bash
# Using Homebrew
brew install python@3.11
```

#### Linux
```bash
sudo apt install python3.11 python3.11-pip python3.11-venv
```

### Create Virtual Environment

```bash
# Navigate to project directory
cd /path/to/data_quality_framework_perfecto2

# Create virtual environment
python -m venv final_env

# Activate virtual environment
# Windows:
final_env\Scripts\activate
# macOS/Linux:
source final_env/bin/activate

# Verify activation (should show virtual env name)
which python
```

---

## 4. Framework Installation

### Install Dependencies

```bash
# Ensure virtual environment is activated
pip install --upgrade pip

# Install core dependencies
pip install pandas==2.0.3
pip install psycopg2-binary==2.9.7
pip install great-expectations==0.17.23
pip install reportlab==4.0.4
pip install Pillow==10.0.0
pip install requests==2.31.0

# Install email dependencies
pip install secure-smtplib==0.1.1

# Install additional utilities
pip install python-dotenv==1.0.0
pip install colorama==0.4.6
pip install tabulate==0.9.0

# For development/testing
pip install pytest==7.4.2
pip install pytest-cov==4.1.0
```

### Alternative: Install from requirements.txt

Create `requirements.txt`:
```txt
pandas==2.0.3
psycopg2-binary==2.9.7
great-expectations==0.17.23
reportlab==4.0.4
Pillow==10.0.0
requests==2.31.0
secure-smtplib==0.1.1
python-dotenv==1.0.0
colorama==0.4.6
tabulate==0.9.0
pytest==7.4.2
pytest-cov==4.1.0
```

Then install:
```bash
pip install -r requirements.txt
```

---

## 5. Configuration

### Database Configuration

1. **Edit `config/settings.py`**:
   ```python
   POSTGRES_CONFIG = {
       'connection': {
           'host': 'localhost',
           'port': 5432,
           'database': 'data_quality_db',
           'user': 'dq_user',
           'password': 'dq_password'
       }
   }
   
   # Database connections for data sources
   DATABASE_CONFIGS = {
       'dev_db': {
           'host': 'localhost',
           'port': 5432,
           'database': 'dev_database',
           'user': 'dev_user',
           'password': 'dev_password'
       },
       'prod_db': {
           'host': 'localhost',
           'port': 5432,
           'database': 'prod_database',
           'user': 'prod_user',
           'password': 'prod_password'
       }
   }
   ```

2. **Configure Email Settings** (optional):
   ```python
   EMAIL_CONFIG = {
       'smtp_server': 'smtp.gmail.com',
       'smtp_port': 587,
       'sender_email': 'your-email@gmail.com',
       'sender_password': 'your-app-password',
       'recipients': ['stakeholder1@company.com', 'stakeholder2@company.com']
   }
   ```

### Environment Variables (Alternative)

Create `.env` file:
```env
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=data_quality_db
POSTGRES_USER=dq_user
POSTGRES_PASSWORD=dq_password

# Email Configuration
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your-email@gmail.com
SENDER_PASSWORD=your-app-password
```

---

## 6. Apache Superset Setup

### Option A: pip Installation (Recommended)

```bash
# Create separate environment for Superset
python -m venv superset_env

# Activate Superset environment
# Windows:
superset_env\Scripts\activate
# macOS/Linux:
source superset_env/bin/activate

# Install Superset
pip install apache-superset==3.0.0

# Install PostgreSQL driver
pip install psycopg2-binary

# Initialize Superset
superset db upgrade

# Create admin user
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin

# Load examples (optional)
superset load_examples

# Initialize Superset
superset init
```

### Option B: Docker Installation (Alternative)

```bash
# Clone Superset repository
git clone https://github.com/apache/superset.git
cd superset

# Start with Docker Compose
docker-compose -f docker-compose-non-dev.yml up -d

# Access at http://localhost:8088
# Default credentials: admin/admin
```

### Start Superset

```bash
# Activate Superset environment
source superset_env/bin/activate

# Start Superset server
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
```

Access Superset at: http://localhost:8088
- Username: `admin`
- Password: `admin`

---

## 7. First Run and Testing

### Verify Database Connection

```bash
# Activate framework environment
source final_env/bin/activate

# Test database connection
python dashboards/verify_database.py
```

Expected output:
```
🔍 Verifying PostgreSQL Database for Superset Dashboards
============================================================
✅ Successfully connected to PostgreSQL
✅ Schema 'data_quality' exists
✅ All tables exist with required columns
✅ Sample queries executed successfully
🎉 Database is ready for Superset dashboards!
```

### Run Data Quality Framework

```bash
# First run to create tables and populate data
python main.py
```

Expected output:
```
🚀 Starting Data Quality Validation Framework
============================================================
📊 Processing dev_db.tva_due...
📊 Processing dev_db.dts_recap...
📊 Processing dev_db.titre_participation...
📊 Processing prod_db.tva_due...
📊 Processing prod_db.dts_recap...
📊 Processing prod_db.titre_participation...

📋 Summary Report:
✅ Total tables processed: 6
✅ Table metrics stored: 6
✅ Database metrics stored: 2
✅ Dimension scores stored: 18
📧 Email report sent successfully
```

### Verify Data Storage

```sql
-- Connect to PostgreSQL
psql -U dq_user -h localhost -p 5432 -d data_quality_db

-- Check tables
\dt data_quality.*

-- Verify data
SELECT COUNT(*) FROM data_quality.summary_metrics_table;
SELECT COUNT(*) FROM data_quality.summary_metrics_database;
SELECT COUNT(*) FROM data_quality.dimension_scores;

-- Sample dimension scores
SELECT database_name, table_name, test_dimension, dimension_score 
FROM data_quality.dimension_scores 
ORDER BY dimension_score DESC;
```

---

## 8. Dashboard Creation

### Manual Setup in Superset UI

1. **Add Database Connection**:
   - Go to Settings → Database Connections
   - Click "+ Database"
   - Select PostgreSQL
   - Connection details:
     ```
     Host: localhost
     Port: 5432
     Database: data_quality_db
     Username: dq_user
     Password: dq_password
     ```

2. **Test Connection** and Save

### Automated Dashboard Creation

```bash
# Create all 7 dashboards automatically
python dashboards/create_simple_dashboards.py
```

Expected output:
```
🚀 Creating 7 Simple Data Quality Dashboards
============================================================
✅ Successfully created 7 dashboards!

📊 Table Dashboards (6):
   • Data Quality - dev_db.tva_due
   • Data Quality - dev_db.dts_recap
   • Data Quality - dev_db.titre_participation
   • Data Quality - prod_db.tva_due
   • Data Quality - prod_db.dts_recap
   • Data Quality - prod_db.titre_participation

🏢 Overall Dashboards (1):
   • Data Quality - Overall Summary

🎉 Access all dashboards at: http://localhost:8088
```

---

## 9. Troubleshooting

### Common Issues and Solutions

#### PostgreSQL Connection Issues

**Error**: `psycopg2.OperationalError: could not connect to server`

**Solutions**:
```bash
# Check if PostgreSQL is running
# Windows:
net start postgresql-x64-15

# macOS:
brew services start postgresql@15

# Linux:
sudo systemctl start postgresql

# Check port availability
netstat -an | grep 5432
```

#### Python Import Errors

**Error**: `ModuleNotFoundError: No module named 'pandas'`

**Solutions**:
```bash
# Ensure virtual environment is activated
source final_env/bin/activate  # or final_env\Scripts\activate on Windows

# Reinstall dependencies
pip install -r requirements.txt

# Check Python path
which python
```

#### Superset Issues

**Error**: `superset: command not found`

**Solutions**:
```bash
# Activate Superset environment
source superset_env/bin/activate

# Reinstall Superset
pip install apache-superset

# Check installation
superset version
```

#### Permission Issues

**Error**: `PermissionError: [Errno 13] Permission denied`

**Solutions**:
```bash
# Windows: Run as Administrator
# macOS/Linux: Check file permissions
chmod +x main.py

# Or run with python explicitly
python main.py
```

### Log Files and Debugging

1. **Enable Debug Logging**:
   ```python
   # In config/settings.py
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Check Log Files**:
   - Framework logs: Console output
   - PostgreSQL logs: Check PostgreSQL log directory
   - Superset logs: `~/.superset/superset.log`

---

## 10. Maintenance

### Regular Tasks

#### Daily
- Monitor dashboard data freshness
- Check email reports
- Verify framework execution logs

#### Weekly
- Review data quality trends
- Update test configurations if needed
- Clean up old log files

#### Monthly
- Update dependencies
- Review and optimize database performance
- Backup configuration files

### Database Maintenance

```sql
-- Clean old data (keep last 90 days)
DELETE FROM data_quality.summary_metrics_table 
WHERE execution_timestamp < CURRENT_DATE - INTERVAL '90 days';

DELETE FROM data_quality.summary_metrics_database 
WHERE execution_timestamp < CURRENT_DATE - INTERVAL '90 days';

DELETE FROM data_quality.dimension_scores 
WHERE execution_timestamp < CURRENT_DATE - INTERVAL '90 days';

-- Analyze tables for performance
ANALYZE data_quality.summary_metrics_table;
ANALYZE data_quality.summary_metrics_database;
ANALYZE data_quality.dimension_scores;
```

### Backup Strategy

```bash
# Backup PostgreSQL database
pg_dump -U dq_user -h localhost -p 5432 data_quality_db > backup_$(date +%Y%m%d).sql

# Backup configuration files
tar -czf config_backup_$(date +%Y%m%d).tar.gz config/

# Backup Superset metadata
# (Superset stores metadata in its own database)
```

### Updates and Upgrades

```bash
# Update Python dependencies
pip list --outdated
pip install --upgrade package_name

# Update Superset
pip install --upgrade apache-superset
superset db upgrade
```

---

## 🎉 Congratulations!

You now have a fully functional data quality framework with:

- ✅ **PostgreSQL Database**: Storing test results, metrics, and dimension scores
- ✅ **Python Framework**: Running automated data quality checks
- ✅ **Apache Superset**: Providing interactive dashboards
- ✅ **Email Reports**: Automated PDF reports for stakeholders
- ✅ **7 Dashboards**: Per-table, per-database, and overall views

### Next Steps

1. **Customize Tests**: Add your specific data quality rules
2. **Schedule Execution**: Set up cron jobs or task scheduler
3. **Extend Dashboards**: Add more visualizations as needed
4. **Monitor Performance**: Track execution times and optimize
5. **Scale Up**: Add more databases and tables as required

### Support

For issues or questions:
1. Check the troubleshooting section above
2. Review log files for error details
3. Consult the official documentation:
   - [PostgreSQL Docs](https://www.postgresql.org/docs/)
   - [Apache Superset Docs](https://superset.apache.org/docs/intro)
   - [Great Expectations Docs](https://docs.greatexpectations.io/)

---

**Happy Data Quality Monitoring!** 📊✨
