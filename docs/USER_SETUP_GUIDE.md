# 🚀 Data Quality Framework - User Setup Guide

## 📋 Prerequisites (Software to Install)

Before starting, make sure you have these installed:
- **Python 3.8+** 
- **PostgreSQL** (for storing results and historical data)
- **Apache Hive/Hadoop** (your data source)
- **Git** (to clone the repository)

## 🔧 Configuration Steps

### Step 1: Database Connection Setup

#### 1.1 Configure Hive Connection
**File:** `config/settings.py`
**Lines:** 20-30

```python
HIVE_CONFIG = {
    'host': 'YOUR_HIVE_HOST',           # Change this to your Hive server IP/hostname
    'port': 10000,                      # Change if your Hive uses different port
    'username': 'YOUR_USERNAME',        # Your Hive username
    'password': 'YOUR_PASSWORD',        # Your Hive password (optional)
    'database': 'default',              # Default database to connect to
    'auth': 'NOSASL'                    # Change to 'KERBEROS' if using Kerberos
}
```

**What to change:**
- Replace `YOUR_HIVE_HOST` with your actual Hive server address
- Replace `YOUR_USERNAME` with your Hive username
- Replace `YOUR_PASSWORD` with your password (if required)

#### 1.2 Configure PostgreSQL Connection
**File:** `config/settings.py`
**Lines:** 32-45

```python
POSTGRES_CONFIG = {
    'enabled': True,                    # Set to False if you don't want to use PostgreSQL
    'connection': {
        'host': 'localhost',            # Change to your PostgreSQL server
        'port': 5432,                   # Change if using different port
        'database': 'data_quality',     # Change to your database name
        'user': 'YOUR_PG_USERNAME',     # Your PostgreSQL username
        'password': 'YOUR_PG_PASSWORD', # Your PostgreSQL password
        'schema': 'data_quality'        # Schema where tables will be created
    },
    'auto_create_tables': True          # Automatically create required tables
}
```

**What to change:**
- Replace `YOUR_PG_USERNAME` with your PostgreSQL username
- Replace `YOUR_PG_PASSWORD` with your PostgreSQL password
- Change `host` if PostgreSQL is on a different server
- Change `database` to match your PostgreSQL database name

### Step 2: Configure Your Databases and Tables

#### 2.1 Define Your Databases
**File:** `config/settings.py`
**Lines:** 47-52

```python
DATABASES = ['preprocessed_dev', 'preprocessed_prod']  # Add your database names here
```

**What to change:**
- Replace with your actual Hive database names
- Add or remove databases as needed

#### 2.2 Define Your Tables
**File:** `config/settings.py`
**Lines:** 54-59

```python
TABLES = [
    'dts_recap',
    'titre_participation', 
    'tva_due'
]  # Add your table names here
```

**What to change:**
- Replace with your actual table names
- Add or remove tables as needed

#### 2.3 Configure Database-Table Combinations
**File:** `config/settings.py**
**Lines:** 61-75

```python
DATABASE_TABLE_CONFIG = {
    'preprocessed_dev': ['dts_recap', 'titre_participation', 'tva_due'],
    'preprocessed_prod': ['dts_recap', 'titre_participation', 'tva_due']
}
```

**What to change:**
- Replace database names with your actual databases
- For each database, list which tables you want to check
- You can have different tables for different databases

### Step 3: Configure Table Schemas

#### 3.1 Define Expected Schemas
**File:** `config/schemas.py`
**Lines:** 10-50

For each table, define its expected schema:

```python
SCHEMAS = {
    'your_table_name': {
        'column1': 'string',
        'column2': 'bigint',
        'column3': 'double',
        'date_column': 'date'
    }
}
```

**What to change:**
- Replace `your_table_name` with your actual table names
- List all columns and their expected data types
- Use Hive data types: `string`, `bigint`, `int`, `double`, `float`, `date`, `timestamp`, etc.

**Example for a real table:**
```python
SCHEMAS = {
    'customer_data': {
        'customer_id': 'bigint',
        'customer_name': 'string',
        'email': 'string',
        'registration_date': 'date',
        'total_orders': 'int',
        'total_spent': 'double'
    },
    'order_details': {
        'order_id': 'bigint',
        'customer_id': 'bigint',
        'product_name': 'string',
        'quantity': 'int',
        'price': 'double',
        'order_date': 'timestamp'
    }
}
```

### Step 4: Configure Quality Checks

#### 4.1 Row Count Thresholds
**File:** `config/settings.py`
**Lines:** 114-121

```python
'completeness': {
    'enable_null_checks': True,
    'enable_row_count_check': True,
    'row_count_minimum': 2,              # Fallback minimum when no history
    'dynamic_row_count': {
        'enabled': True,                 # Enable smart thresholds
        'tolerance_percentage': 20.0,    # ±20% tolerance for predictions
        'minimum_history_required': 2,   # Need 2+ records for smart calculation
        'lookback_executions': 5,        # Look at last 5 runs
        'fallback_minimum': 2            # Minimum when no history
    }
}
```

**What to change:**
- `row_count_minimum`: Change the fallback minimum row count
- `tolerance_percentage`: Adjust how strict the dynamic thresholds are (lower = stricter)
- `minimum_history_required`: How many historical records needed for smart calculation
- Set `enabled: False` if you want fixed thresholds only

#### 4.2 Enable/Disable Specific Checks
**File:** `config/settings.py`
**Lines:** 110-144

```python
QUALITY_CHECKS = {
    'completeness': {
        'enable_null_checks': True,          # Check for columns with all NULL values
        'enable_row_count_check': True,      # Check minimum row counts
    },
    'consistency': {
        'enable_schema_checks': True,        # Check table structure
        'enable_schema_presence_check': True, # Check if columns exist
        'enable_schema_types_check': True    # Check column data types
    },
    'timeliness': {
        'enable_date_freshness': True        # Check if data is recent
    },
    'uniqueness': {
        'enable_row_uniqueness_check': True  # Check for duplicate rows
    }
}
```

**What to change:**
- Set any check to `False` to disable it
- All checks are enabled by default

### Step 5: Configure Email Notifications (Optional)

#### 5.1 Email Settings
**File:** `config/settings.py`
**Lines:** Look for `EMAIL_CONFIG`

```python
EMAIL_CONFIG = {
    'enabled': False,                    # Set to True to enable emails
    'smtp': {
        'server': 'smtp.gmail.com',      # Your SMTP server
        'port': 587,                     # SMTP port
        'username': 'your-email@gmail.com',  # Your email
        'password': 'your-app-password',     # Your email password/app password
        'use_tls': True
    },
    'recipients': {
        'to': ['recipient1@company.com', 'recipient2@company.com'],
        'cc': [],                        # Optional CC recipients
        'bcc': []                        # Optional BCC recipients
    },
    'test_mode': True                    # Set False for production
}
```

**What to change:**
- Set `enabled: True` to activate email notifications
- Replace SMTP settings with your email provider's settings
- Add recipient email addresses
- Set `test_mode: False` for production use

### Step 6: Configure Output Settings

#### 6.1 Report Generation
**File:** `config/settings.py`
**Lines:** Look for `OUTPUT_CONFIG`

```python
OUTPUT_CONFIG = {
    'enable_json_output': True,          # Generate JSON reports
    'enable_pdf_output': True,           # Generate PDF reports
    'enable_console_output': True,       # Show results in console
    'json_output_dir': 'results',        # Where to save JSON files
    'pdf_output_dir': 'reports'          # Where to save PDF files
}
```

**What to change:**
- Set any output to `False` to disable it
- Change directory names if you want different folders

## 🏃‍♂️ Running the Framework

### Basic Run
```bash
python main.py
```

### Run with Logging to File
```powershell
python main.py 2>&1 | Tee-Object -FilePath "data_quality_log.txt"
```

### Run for Specific Databases/Tables
Edit `main.py` and modify the `framework.run()` call:

```python
# Run for specific databases
framework.run(databases=['preprocessed_dev'])

# Run for specific tables
framework.run(tables=['customer_data', 'order_details'])

# Run for specific database-table combinations
framework.run(databases=['preprocessed_dev'], tables=['customer_data'])
```

## 📊 Understanding Results

### Console Output
- `[SUCCESS]` - Test passed
- `[ERROR]` - Test failed  
- `[WARNING]` - Non-critical issue
- `[INFO]` - General information

### Generated Files
- **JSON Reports**: `results/data_quality_results_YYYYMMDD_HHMMSS.json`
- **PDF Reports**: `reports/data_quality_report_YYYYMMDD_HHMMSS.pdf`
- **Log Files**: `data_quality_log.txt` (if using logging command)

### PostgreSQL Tables
If PostgreSQL is enabled, results are stored in:
- `data_quality.summary_metrics` - Overall test results
- `data_quality.dimension_scores` - Scores by data quality dimension
- `data_quality.freshness_data` - Data freshness information
- `data_quality.row_count_history` - Historical row counts for smart thresholds
