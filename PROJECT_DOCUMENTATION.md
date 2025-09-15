# 📋 Data Quality Framework - Technical Documentation

## 📑 Table of Contents
1. [🎯 Overview](#-overview)
2. [🏗️ Architecture](#️-architecture)
3. [🔧 Core Components](#-core-components)
4. [📊 Quality Dimensions](#-quality-dimensions)
5. [⚙️ Configuration System](#️-configuration-system)
6. [🔍 Quality Validation Engine](#-quality-validation-engine)
7. [📈 Reporting & Analytics](#-reporting--analytics)
8. [💾 Data Storage & Persistence](#-data-storage--persistence)
9. [🤖 Automation & Scheduling](#-automation--scheduling)
10. [🔄 Data Flow & Processing](#-data-flow--processing)
11. [🛠️ Technical Implementation](#️-technical-implementation)
12. [📚 API Reference](#-api-reference)

---

## 🎯 Overview

The Data Quality Framework is an enterprise-grade, multi-environment data validation system designed for monitoring and ensuring data quality across Hive clusters. Built with Great Expectations at its core, it provides comprehensive validation, cross-environment comparison, historical trend analysis, and professional reporting capabilities.

### 🚀 Key Capabilities
- **Multi-Environment Validation**: Simultaneous DEV and PROD environment monitoring
- **Dynamic Quality Checks**: Adaptive thresholds based on historical data patterns
- **Cross-Environment Comparison**: Automated DEV ≤ PROD row count validation
- **Historical Trend Analysis**: Time-series data quality tracking and predictive validation
- **Professional Reporting**: Executive-level PDF reports with visual analytics
- **Real-Time Monitoring**: Live validation with immediate feedback and alerting
- **Scalable Architecture**: Handles partitioned tables and large datasets efficiently
- **Automated Scheduling**: Airflow integration with comprehensive error handling

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DATA QUALITY FRAMEWORK                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│  DEV Environment          │    Framework Core    │     PROD Environment         │
│  ┌─────────────────┐     │  ┌─────────────────┐ │     ┌─────────────────┐      │
│  │ Hive DEV        │────►│  │ DataQualityFW   │ │◄────│ Hive PROD       │      │
│  │ - preprocessed  │     │  │ - Validation    │ │     │ - preprocessed  │      │
│  │ - test          │     │  │ - Comparison    │ │     │ - test          │      │
│  └─────────────────┘     │  │ - Analysis      │ │     └─────────────────┘      │
│                          │  └─────────────────┘ │                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                    ┌─────────────────┐    ┌─────────────────┐                   │
│                    │   PostgreSQL    │    │    Outputs      │                   │
│                    │ - Metrics       │    │ - PDF Reports   │                   │
│                    │ - History       │    │ - JSON Results  │                   │
│                    │ - Trends        │    │ - Notifications │                   │
│                    └─────────────────┘    └─────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 🔄 Data Flow Process
1. **Multi-Environment Connection**: Simultaneous DEV and PROD Hive cluster access
2. **Intelligent Discovery**: Auto-detection of tables, partitions, and schemas
3. **Parallel Processing**: Concurrent validation across environments
4. **Cross-Environment Analysis**: DEV vs PROD comparison with compliance rules
5. **Historical Integration**: Trend analysis using PostgreSQL time-series data
6. **Professional Reporting**: Executive PDF reports with visual analytics
7. **Automated Distribution**: Email notifications and result storage

---

## 🔧 Core Components

### **DataQualityFramework** (`main.py`)
- **Role**: Main orchestrator and workflow manager
- **Responsibilities**: Environment coordination, validation execution, result aggregation
- **Features**: Multi-environment processing, error handling, progress tracking

### **HiveConnectionManager** (`core/connection.py`)
- **Role**: Database connectivity and session management
- **Responsibilities**: Connection pooling, authentication, environment switching
- **Features**: Auto-reconnection, connection validation, resource cleanup

### **DataFetcher** (`core/data_fetcher.py`)
- **Role**: Data retrieval and metadata extraction
- **Responsibilities**: Table sampling, partition handling, schema discovery
- **Features**: Optimized queries, memory management, partition detection

### **QualityChecker** (`core/quality_checks.py`)
- **Role**: Validation engine using Great Expectations
- **Responsibilities**: Rule execution, threshold calculation, result scoring
- **Features**: Dynamic thresholds, custom validations, historical comparison

### **PDFReportGenerator** (`reporting/pdf_generator.py`)
- **Role**: Professional report generation
- **Responsibilities**: Executive summaries, visual charts, compliance reporting
- **Features**: Multi-database tables, environment sections, trend visualization

### **PostgreSQLStorage** (`storage/postgres_storage.py`)
- **Role**: Data persistence and historical tracking
- **Responsibilities**: Metrics storage, trend analysis, result archival
- **Features**: Time-series data, automated cleanup, performance optimization

---

## 📊 Quality Dimensions

### **Completeness** 🔍
- **Row Count Validation**: Dynamic thresholds based on historical patterns
- **Null Value Detection**: Identification of excessive missing data
- **Data Availability**: Verification of expected data presence
- **Incremental Analysis**: Growth pattern validation using recent trends

### **Consistency** ⚖️
- **Schema Validation**: Column existence and data type verification
- **Format Compliance**: Data format and structure validation
- **Cross-Environment Consistency**: DEV vs PROD schema alignment
- **Referential Integrity**: Relationship validation across tables

### **Timeliness** ⏰
- **Data Freshness**: `date_insertion` recency validation
- **Update Frequency**: Expected refresh pattern compliance
- **Lag Detection**: Identification of data processing delays
- **Temporal Trends**: Time-based quality pattern analysis

### **Uniqueness** 🎯
- **Duplicate Detection**: Row-level uniqueness validation
- **Key Constraint Validation**: Primary key integrity checks
- **Data Deduplication**: Identification of redundant records
- **Cardinality Analysis**: Expected uniqueness pattern validation

## ⚙️ Configuration System

### **Multi-Environment Setup** (`config/settings.py`)

```python
# Hive Environment Connections
HIVE_ENVIRONMENTS = {
    'DEV': {
        'host': 'dev-hive-cluster.company.com',
        'port': 10000,
        'username': 'data_quality_user',
        'database': 'default'
    },
    'PROD': {
        'host': 'prod-hive-cluster.company.com',
        'port': 10000,
        'username': 'data_quality_user',
        'database': 'default'
    }
}

# Database and Table Configuration
DATABASE_TABLE_CONFIG = {
    'preprocessed': ['tva_due', 'dts_recap', 'titre_participation', 'personnes'],
    'test': ['table_test']
}

# Advanced Quality Check Settings
QUALITY_CHECKS = {
    'enable_schema_validation': True,
    'enable_row_count_validation': True,
    'enable_null_validation': True,
    'enable_freshness_validation': True,
    'enable_duplicate_detection': True,
    'enable_row_count_comparison': True,
    'row_count_tolerance_percentage': 10.0,
    'freshness_threshold_days': 7,
    'null_tolerance_percentage': 5.0
}

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    'host': 'postgres-server.company.com',
    'port': 5432,
    'database': 'data_quality_metrics',
    'username': 'dq_user',
    'password': 'secure_password'
}
```

### **Schema Definitions** (`config/schemas.py`)

```python
TABLE_SCHEMAS = {
    "tva_due": {
        "date_insertion": "timestamp",
        "batch_id": "bigint",
        "systeme_source": "string",
        "montant_tva": "decimal(10,2)"
    },
    "personnes": {
        "date_insertion": "timestamp",
        "person_id": "bigint",
        "nom": "string",
        "prenom": "string"
    }
}
```

---

## 🔍 Quality Validation Engine

### **Dynamic Row Count Validation**
- **Historical Analysis**: Uses PostgreSQL time-series data for trend calculation
- **Incremental Method**: Calculates expected growth based on recent patterns
- **Adaptive Thresholds**: Adjusts validation rules based on data behavior
- **Cross-Environment Rules**: Enforces DEV ≤ PROD compliance automatically

### **Great Expectations Integration**
- **Expectation Suites**: Automated generation based on table schemas
- **Custom Validators**: Business-specific validation rules
- **Batch Processing**: Efficient validation of large datasets
- **Result Aggregation**: Comprehensive scoring and reporting

### **Validation Workflow**
1. **Schema Discovery**: Auto-detection of table structure and partitions
2. **Historical Context**: Retrieval of previous validation results and trends
3. **Threshold Calculation**: Dynamic computation based on historical patterns
4. **Parallel Execution**: Concurrent validation across environments
5. **Result Scoring**: Quality dimension scoring and overall assessment
6. **Cross-Environment Analysis**: DEV vs PROD comparison and compliance checking

---

## 📈 Reporting & Analytics

### **Executive PDF Reports**
- **Multi-Database Organization**: Separate sections per database with comparison tables
- **Environment Analysis**: Dedicated DEV and PROD result sections
- **Row Count Comparison**: Organized tables showing count differences and compliance
- **Visual Indicators**: Color-coded status (✅ PASS, ❌ FAIL) with trend charts
- **Quality Dimensions**: Detailed breakdown by validation category

### **Report Structure**
```
📊 Executive Summary
├── Overall Quality Score
├── Environment Comparison
└── Key Findings

🗄️ Database: preprocessed
├── Table: tva_due (✅ PASS)
├── Table: personnes (❌ FAIL)
└── Row Count Analysis

🗄️ Database: test  
├── Table: table_test (✅ PASS)
└── Row Count Analysis

📋 Quality Dimensions
├── Completeness: 85%
├── Consistency: 92%
├── Timeliness: 78%
└── Uniqueness: 96%
```

### **Data Outputs**
- **JSON Results**: Machine-readable validation results in `results/` directory
- **PostgreSQL Storage**: Historical metrics for dashboard integration
- **Real-Time Logging**: Detailed console output with progress tracking

---

## 💾 Data Storage & Persistence

### **PostgreSQL Schema**
```sql
-- Overall quality metrics per table/environment
CREATE TABLE summary_metrics_table (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    database_name VARCHAR(50),
    environment VARCHAR(10),
    overall_score DECIMAL(5,2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Quality scores by dimension
CREATE TABLE dimension_scores (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    database_name VARCHAR(50),
    environment VARCHAR(10),
    test_dimension VARCHAR(50),
    score DECIMAL(5,2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Historical row counts for trend analysis
CREATE TABLE row_count_history (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    database_name VARCHAR(50),
    environment VARCHAR(10),
    row_count BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data freshness tracking
CREATE TABLE freshness (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    database_name VARCHAR(50),
    environment VARCHAR(10),
    latest_date TIMESTAMP,
    row_count BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### **Storage Benefits**
- **Historical Trending**: Time-series analysis for predictive validation
- **Dashboard Integration**: Compatible with Superset, Grafana, and Tableau
- **Automated Alerting**: Threshold-based notifications and monitoring
- **Performance Analytics**: Query optimization and execution metrics

---

## 🤖 Automation & Scheduling

### **Airflow DAG Implementation**
```python
# airflow_dag.py - Production-ready scheduling
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_quality_framework',
    default_args=default_args,
    description='Enterprise Data Quality Validation',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    max_active_runs=1
)
```

### **Scheduling Features**
- **Environment Validation**: Pre-flight dependency and connectivity checks
- **Comprehensive Logging**: Detailed execution logs with automatic cleanup
- **Error Handling**: Intelligent retry logic with escalation procedures
- **Notification System**: Email alerts with PDF report attachments
- **Resource Management**: Memory and connection pool optimization

---

## 🔄 Data Flow & Processing

### **Processing Pipeline**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Configuration  │───►│   Connection    │───►│  Data Discovery │
│  - Environments │    │  - DEV Hive     │    │  - Tables       │
│  - Tables       │    │  - PROD Hive    │    │  - Partitions   │
│  - Thresholds   │    │  - PostgreSQL   │    │  - Schemas      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Reporting     │◄───│   Validation    │◄───│  Data Sampling  │
│  - PDF Reports  │    │  - GX Checks    │    │  - Row Counts   │
│  - JSON Output  │    │  - Comparisons  │    │  - Metadata     │
│  - Notifications│    │  - Scoring      │    │  - Quality Data │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### **Performance Optimizations**
- **Parallel Processing**: Concurrent environment validation
- **Memory Management**: Efficient data sampling and processing
- **Connection Pooling**: Optimized database connectivity
- **Incremental Processing**: Delta-based validation for large datasets

---

## 🛠️ Technical Implementation

### **Technology Stack**
- **Core Framework**: Python 3.9+ with Great Expectations
- **Database Connectivity**: PyHive, SQLAlchemy, psycopg2
- **Report Generation**: ReportLab for professional PDF creation
- **Scheduling**: Apache Airflow with comprehensive DAG management
- **Data Storage**: PostgreSQL for metrics and historical data

### **Key Design Patterns**
- **Factory Pattern**: Environment-specific connection management
- **Observer Pattern**: Real-time progress tracking and notifications
- **Strategy Pattern**: Configurable validation rules and thresholds
- **Template Pattern**: Consistent report generation across environments

### **Error Handling & Resilience**
- **Graceful Degradation**: Partial execution with detailed error reporting
- **Retry Logic**: Intelligent reconnection and validation retry mechanisms
- **Resource Cleanup**: Automatic connection and memory management
- **Comprehensive Logging**: Detailed execution tracking for troubleshooting

---

## 📚 API Reference

### **Main Classes**

#### `DataQualityFramework`
```python
class DataQualityFramework:
    def __init__(self):
        """Initialize framework with configuration"""
    
    def run_all_checks(self, combinations=None) -> bool:
        """Execute quality checks for specified combinations"""
    
    def compare_row_counts_between_environments(self):
        """Cross-environment row count analysis"""
```

#### `HiveConnectionManager`
```python
class HiveConnectionManager:
    def get_connection(self, environment: str):
        """Get Hive connection for specified environment"""
    
    def test_connection(self, environment: str) -> bool:
        """Validate connectivity to Hive cluster"""
```

#### `QualityChecker`
```python
class QualityChecker:
    def run_quality_checks(self, df, table_name, environment, database):
        """Execute Great Expectations validation suite"""
    
    def calculate_incremental_threshold(self, table_name, database, environment):
        """Dynamic threshold calculation using historical data"""
```

### **Configuration Parameters**
- `HIVE_ENVIRONMENTS`: Multi-environment connection settings
- `DATABASE_TABLE_CONFIG`: Table and database mapping configuration
- `QUALITY_CHECKS`: Validation rule enablement and threshold settings
- `POSTGRES_CONFIG`: Result storage and historical data configuration

