# 🔍 Data Quality Framework

A comprehensive, enterprise-grade data quality framework for monitoring and validating Hive tables across DEV and PROD environments using Great Expectations. Features automated quality checks, environment comparisons, historical trend analysis, and professional PDF reporting.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Access to Hive cluster (DEV and PROD environments)
- PostgreSQL database (for storing results)
- Git (for cloning the repository)

### 1. Clone Repository
```bash
git clone <repository-url>
cd data_quality_framework
```

## 🖥️ Environment Setup

### Windows Setup

#### Step 1: Install Python 3.9+
1. Download Python from [python.org](https://www.python.org/downloads/)
2. During installation, check "Add Python to PATH"
3. Verify installation:
```cmd
python --version
pip --version
```

#### Step 2: Create Virtual Environment
```cmd
# Navigate to project directory
cd data_quality_framework

# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate

# Verify activation (should show (venv) in prompt)
```

#### Step 3: Install Dependencies
```cmd
# Upgrade pip
python -m pip install --upgrade pip

# Install required packages
pip install -r requirements.txt

# Verify installation
pip list
```

### Linux/Unix Setup

#### Step 1: Install Python 3.9+
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.9 python3.9-venv python3-pip

# CentOS/RHEL
sudo yum install python39 python39-pip

# Verify installation
python3.9 --version
pip3 --version
```

#### Step 2: Create Virtual Environment
```bash
# Navigate to project directory
cd data_quality_framework

# Create virtual environment
python3.9 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Verify activation (should show (venv) in prompt)
```

#### Step 3: Install Dependencies
```bash
# Upgrade pip
python -m pip install --upgrade pip

# Install required packages
pip install -r requirements.txt

# Verify installation
pip list
```

## ⚙️ Configuration Setup

### Step 1: Configure Hive Connections
Edit `config/settings.py` and update the Hive environment configurations:

```python
HIVE_ENVIRONMENTS = {
    'DEV': {
        'host': 'your-dev-hive-host.com',
        'port': 10000,
        'username': 'your-username',
        'database': 'default'
    },
    'PROD': {
        'host': 'your-prod-hive-host.com', 
        'port': 10000,
        'username': 'your-username',
        'database': 'default'
    }
}
```

### Step 2: Configure Database and Tables
Update the `DATABASE_TABLE_CONFIG` in `config/settings.py`:

```python
DATABASE_TABLE_CONFIG = {
    'preprocessed': ['tva_due', 'dts_recap', 'titre_participation', 'personnes'],
    'test': ['table_test']
}
```

### Step 3: Configure Table Schemas
Update the table schema definitions in `config/schemas.py` to match your actual table structures:

```python
# Example: Update schema for your tables
TABLE_SCHEMAS = {
    'tva_due': {
        'columns': ['id', 'amount', 'date_insertion', 'status'],
        'data_types': ['bigint', 'decimal', 'timestamp', 'string'],
        'nullable': [False, False, False, True]
    },
    'your_table_name': {
        'columns': ['col1', 'col2', 'date_insertion'],
        'data_types': ['string', 'bigint', 'timestamp'],
        'nullable': [False, False, False]
    }
}
```

**Important**: The framework expects all tables to have a `date_insertion` column for freshness validation. Update your schemas to reflect your actual table structures.

### Step 4: Configure PostgreSQL
If using PostgreSQL for result storage, update `POSTGRES_CONFIG`:

```python
POSTGRES_CONFIG = {
    'host': 'your-postgres-host',
    'port': 5432,
    'database': 'data_quality',
    'username': 'your-username',
    'password': 'your-password'
}
```

### Step 5: Configure Quality Checks
Customize quality check settings in `config/settings.py`:

```python
QUALITY_CHECKS = {
    'enable_schema_validation': True,
    'enable_row_count_validation': True,
    'enable_null_validation': True,
    'enable_freshness_validation': True,
    'enable_duplicate_detection': True,
    'enable_row_count_comparison': True,
    'row_count_tolerance_percentage': 10.0,
    'freshness_threshold_days': 7
}
```

## 🏃‍♂️ Running the Framework

### Run Quality Checks
```bash
# Run all quality checks
python main.py


## 📈 Quality Checks

### Core Validations
- **Schema Validation**: Verifies expected columns exist with correct data types
- **Row Count Validation**: Uses dynamic thresholds based on historical trends and incremental growth patterns
- **Null Value Detection**: Identifies columns with excessive null/empty values
- **Date Freshness**: Validates `date_insertion` column contains recent data within threshold
- **Duplicate Detection**: Finds and reports duplicate rows in tables
- **Environment Comparison**: Compares row counts between DEV and PROD environments (DEV ≤ PROD rule)

### Advanced Features
- **Dynamic Thresholds**: Row count validation adapts to historical data patterns
- **Historical Trend Analysis**: Tracks data growth over time for predictive validation
- **Multi-Environment Support**: Simultaneous validation across DEV and PROD
- **Incremental Method**: Calculates expected row counts based on recent growth patterns

## 📊 Reports & Outputs

### PDF Reports
- **Executive Summary**: High-level overview with pass/fail statistics
- **Environment Sections**: Separate DEV and PROD result sections
- **Row Count Analysis**: Organized by database with comparison tables
- **Quality Dimensions**: Detailed breakdown by validation type
- **Visual Indicators**: Color-coded status (✅ PASS, ❌ FAIL) and trend charts

### Data Outputs
- **JSON Results**: Machine-readable results in `results/` directory
- **Console Logging**: Real-time progress with detailed debugging information
- **PostgreSQL Storage**: Historical data for trend analysis and reporting

## 💾 Storage Schema

### PostgreSQL Tables
```sql
-- Overall quality scores per table/environment
summary_metrics_table (table_name, database, environment, overall_score, timestamp)

-- Quality scores by dimension (completeness, consistency, etc.)
dimension_scores (table_name, database, environment, test_dimension, score, timestamp)

-- Historical row counts for trend analysis
row_count_history (table_name, database, environment, row_count, timestamp)

-- Data freshness tracking
freshness (table_name, database, environment, latest_date, row_count, timestamp)
```

## 🔧 Project Structure

```
data_quality_framework/
├── config/
│   ├── settings.py          # Main configuration (HIVE_ENVIRONMENTS, DATABASE_TABLE_CONFIG)
│   └── schemas.py           # Table schema definitions
├── core/
│   ├── connection.py        # Hive connection management
│   ├── data_fetcher.py      # Data retrieval with partition support
│   └── quality_checks.py    # Great Expectations validation logic
├── reporting/
│   └── pdf_generator.py     # Professional PDF report generation
├── storage/
│   └── postgres_storage.py  # PostgreSQL integration and historical data
├── utils/
│   └── helpers.py           # Common utilities and helper functions
├── examples/
│   ├── sample_config.py     # Configuration examples
│   └── sample_schemas.py    # Schema definition examples
├── docs/                    # Comprehensive documentation
├── main.py                  # Main orchestrator
├── test_connections.py      # Connection testing utility
└── airflow_dag.py          # Apache Airflow scheduling
```

## 🤖 Automation & Scheduling

### Airflow Integration
The included `airflow_dag.py` provides:
- **Daily Execution**: Runs at 6 AM with configurable schedule
- **Environment Validation**: Pre-flight checks for dependencies
- **Comprehensive Logging**: Detailed execution logs with cleanup
- **Error Handling**: Automatic failure notifications and recovery

### Manual Scheduling
```bash
# Run via cron (Linux/Mac)
0 6 * * * /path/to/venv/bin/python /path/to/data_quality_framework/main.py

# Run via Task Scheduler (Windows)
# Create scheduled task pointing to: python.exe main.py
```

