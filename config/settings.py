"""
Configuration settings for the Data Quality Framework
"""

# Hive Connection Settings - Multiple Environments
HIVE_ENVIRONMENTS = {
    'PROD': {
        'host': 'your-prod-hive-host.company.com',
        'port': 10000,
        'username': 'your-username',
        'database': 'default'
    },
    'DEV': {
        'host': 'your-dev-hive-host.company.com', 
        'port': 10000,
        'username': 'your-username',
        'database': 'default'
    }
}

# Database-Table Configuration - Used for both DEV and PROD environments
DATABASE_TABLE_CONFIG = {
    'preprocessed': ['tva_due', 'dts_recap', 'titre_participation', 'personnes'],
    'test': ['table_test']
}

# Function to get all environment-database-table combinations
def get_all_environment_database_table_combinations():
    """
    Get all valid environment-database-table combinations from DATABASE_TABLE_CONFIG
    
    Returns:
        List[tuple]: List of (environment, database, table) tuples
    """
    combinations = []
    # Generate combinations for both DEV and PROD environments
    for environment in ['DEV', 'PROD']:
        for database, tables in DATABASE_TABLE_CONFIG.items():
            for table in tables:
                combinations.append((environment, database, table))
    return combinations

# Legacy function for backward compatibility
def get_all_database_table_combinations():
    """
    Get all valid database-table combinations from DATABASE_TABLE_CONFIG
    
    Returns:
        List[tuple]: List of (database, table) tuples
    """
    combinations = []
    for database, tables in DATABASE_TABLE_CONFIG.items():
        for table in tables:
            combinations.append((database, table))
    return combinations

def get_tables_for_database(database):
    """
    Get all tables configured for a specific database
    
    Args:
        database (str): Database name
        
    Returns:
        List[str]: List of table names for the database
    """
    return DATABASE_TABLE_CONFIG.get(database, [])

def get_databases_for_table(table):
    """
    Get all databases that contain a specific table
    
    Args:
        table (str): Table name
        
    Returns:
        List[str]: List of database names containing the table
    """
    databases = []
    for database, tables in DATABASE_TABLE_CONFIG.items():
        if table in tables:
            databases.append(database)
    return databases

# PostgreSQL Configuration for Results Storage
POSTGRES_CONFIG = {
    'enabled': True,
    'connection': {
        'host': 'your-postgres-host.company.com',
        'superset_host': 'your-superset-host.company.com',
        'port': 5432,
        'database': 'data_quality_db',
        'user': 'your-db-username',
        'password': 'your-secure-password'  # Update with your PostgreSQL password
    },
    'auto_create_tables': True,  # Automatically create tables if they don't exist
    'batch_size': 1000  # Batch size for bulk inserts
}

# Email Configuration for Report Notifications
EMAIL_CONFIG = {
    'enabled': True,
    'smtp': {
        'server': 'smtp.gmail.com',
        'port': 465,
        'username': 'your-email@company.com',
        'password': 'your-app-password',  # App password for Gmail
        'sender_name': 'Data Quality System'
    },
    'recipients': {
        'to': ['data-team@company.com'],  # Primary recipients
        'cc': [],  # CC recipients (optional)
        'bcc': []  # BCC recipients (optional)
    },
    'test_mode': False  # Set to True to test email without sending to all recipients
}

# Quality Check Configuration by Data Quality Dimensions
QUALITY_CHECKS = {
    # Completeness Dimension
    'completeness': {
        'enable_null_checks': True,
        'enable_row_count_check': True,
        'row_count_minimum': 2,
        'dynamic_row_count': {
            'enabled': True,
            'tolerance_percentage': 20.0,  # ±20% tolerance
            'minimum_history_required': 2,  # Need at least 3 historical records
            'lookback_executions': 5,  # Look at last 5 executions for trend calculation
            'fallback_minimum': 2  # Fallback for tables with no history
        }
    },
    
    # Consistency Dimension  
    'consistency': {
        'enable_schema_checks': True,
        'enable_schema_presence_check': True,
        'enable_schema_types_check': True
    },
    
    # Timeliness Dimension
    'timeliness': {
        'enable_date_freshness': True
    },
    
    # Uniqueness Dimension
    'uniqueness': {
        'enable_row_uniqueness_check': True
    },
    
    # Other Checks
    'enable_great_expectations': True,
    'enable_row_count_comparison': True
}

# Row Count Comparison Configuration
ROW_COUNT_COMPARISON = {
    'dev_database': 'preprocessed_dev',
    'prod_database': 'preprocessed_prod',
    'comparison_rule': 'dev_less_than_prod'  # dev < prod
}

# Urgency/Criticality Configuration
URGENCY_CONFIG = {
    # Default thresholds for all tables (can be overridden per table)
    'default_thresholds': {
        'critical': 50.0,    # Below 50% pass rate = CRITICAL
        'high': 70.0,        # 50-70% pass rate = HIGH
        'medium': 85.0,      # 70-85% pass rate = MEDIUM
        'low': 95.0          # 85-95% pass rate = LOW
        # Above 95% = PASS
    },
    
    # Table-specific thresholds (override defaults for specific tables)
    'table_specific': {
        'tva_due': {
            'critical': 80.0,   # Below 80% = CRITICAL for this important table
            'high': 90.0,       # 80-90% = HIGH
            'medium': 95.0,     # 90-95% = MEDIUM
            'low': 98.0         # 95-98% = LOW
        },
        'personnes': {
            'critical': 70.0,   # Below 70% = CRITICAL
            'high': 85.0,       # 70-85% = HIGH
            'medium': 92.0,     # 85-92% = MEDIUM
            'low': 97.0         # 92-97% = LOW
        }
    },
    
    # Urgency level colors and descriptions
    'urgency_levels': {
        'CRITICAL': {
            'color': '#e74c3c',     # Red
            'description': 'Immediate attention required',
            'priority': 1
        },
        'HIGH': {
            'color': '#f39c12',     # Orange
            'description': 'High priority issues',
            'priority': 2
        },
        'MEDIUM': {
            'color': '#f1c40f',     # Yellow
            'description': 'Medium priority issues',
            'priority': 3
        },
        'LOW': {
            'color': '#3498db',     # Blue
            'description': 'Low priority issues',
            'priority': 4
        },
        'PASS': {
            'color': '#27ae60',     # Green
            'description': 'All checks passed',
            'priority': 5
        }
    }
}

# Output Configuration
OUTPUT_CONFIG = {
    'json_output_dir': 'results',
    'pdf_output_dir': 'reports',
    'enable_console_output': True,
    'enable_json_output': False,
    'enable_pdf_output': True
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# Great Expectations Configuration
GX_CONFIG = {
    'context_root_dir': 'gx_temp',
    'datasource_name': 'pandas_datasource',
    'data_connector_name': 'runtime_data_connector'
}

# PDF Report Configuration
PDF_CONFIG = {
    'page_size': 'landscape_letter',
    'title': 'Data Quality Assessment Report',
    'include_summary_chart': True,
    'include_detailed_results': True
}

# OpenMetadata Configuration
OPENMETADATA_CONFIG = {
    'enabled': False,
    'config_file': 'openmetadata.yaml',  # Path relative to project root
    'defaults': {
        'database_service_name': 'dgi_hive',  # Update this to match your OpenMetadata service name
        'schema_name': 'default',  # Default schema name
        'service_type': 'Hive'
    },
    'action_config': {
        'class_name': 'OpenMetadataValidationAction',
        'module_name': 'metadata.great_expectations.action'
    }
}
