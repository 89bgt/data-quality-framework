# Sample Configuration File
# Copy this to config/settings.py and update with your actual values

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
    'preprocessed': ['tva_due', 'dts_recap', 'titre_participation']
}

# PostgreSQL Configuration (for storing results)
POSTGRES_CONFIG = {
    'enabled': True,
    'connection': {
        'host': 'your-postgres-host',
        'port': 5432,
        'database': 'data_quality_db',
        'user': 'dq_user',
        'password': 'your-password'
    },
    'schema': 'data_quality'
}

# Email Configuration
EMAIL_CONFIG = {
    'enabled': True,
    'smtp_server': 'smtp.company.com',
    'smtp_port': 587,
    'username': 'your-email@company.com',
    'password': 'your-app-password',
    'recipients': [
        'data-team@company.com',
        'stakeholders@company.com'
    ],
    'test_mode': False  # Set to True for testing
}

# Quality Check Configuration
QUALITY_CHECKS = {
    'completeness': {
        'enable_row_count_check': True,
        'enable_null_check': True,
        'row_count_minimum': 1000,
        'null_threshold_percentage': 100
    },
    'consistency': {
        'enable_schema_check': True,
        'enable_type_check': True
    },
    'timeliness': {
        'enable_freshness_check': True,
        'freshness_threshold_days': 1
    },
    'uniqueness': {
        'enable_duplicate_check': True,
        'duplicate_threshold_percentage': 5
    }
}

# Output Configuration
OUTPUT_CONFIG = {
    'enable_pdf_output': True,
    'enable_json_output': True,
    'enable_console_output': True,
    'pdf_output_dir': 'reports',
    'json_output_dir': 'results'
}
