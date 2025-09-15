"""
PostgreSQL storage implementation for data quality results and anomalies
Designed for dashboarding with Superset
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd

logger = logging.getLogger(__name__)

class PostgreSQLStorage:
    """PostgreSQL storage for data quality results and anomalies"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize PostgreSQL storage
        
        Args:
            connection_config: PostgreSQL connection configuration
        """
        self.config = connection_config
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config.get('host', 'your-postgres-host'),
                port=self.config.get('port', 5432),
                database=self.config.get('database', 'data_quality_db'),
                user=self.config.get('user'),
                password=self.config.get('password')
            )
            self.connection.autocommit = True
            logger.info("Successfully connected to PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")
    
    def create_tables(self) -> bool:
        """
        Create essential tables for storing data quality results
        Minimal schema for core functionality
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            
            # Create schema if not exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS data_quality;")
            
            
            # 2. Dimension Scores Table - Score per dimension per table per database per environment
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality.dimension_scores (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(50) NOT NULL,
                    execution_timestamp TIMESTAMP NOT NULL,
                    environment VARCHAR(20) NOT NULL,  -- PROD, DEV, etc.
                    database_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100) NOT NULL,
                    test_dimension VARCHAR(50) NOT NULL,  -- completeness, consistency, timeliness, uniqueness
                    total_tests INTEGER NOT NULL DEFAULT 0,
                    passed_tests INTEGER NOT NULL DEFAULT 0,
                    failed_tests INTEGER NOT NULL DEFAULT 0,
                    dimension_score DECIMAL(5,2) NOT NULL DEFAULT 0.00,
                    status VARCHAR(20) NOT NULL DEFAULT 'NO_DATA',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes for dimension_scores
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dimension_scores_timestamp ON data_quality.dimension_scores (execution_timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dimension_scores_environment ON data_quality.dimension_scores (environment);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dimension_scores_database ON data_quality.dimension_scores (database_name);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dimension_scores_table ON data_quality.dimension_scores (table_name);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dimension_scores_dimension ON data_quality.dimension_scores (test_dimension);")
            
            # 3. Table-Level Summary Metrics - Metrics for individual tables per environment
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality.summary_metrics_table (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(50) NOT NULL,
                    execution_timestamp TIMESTAMP NOT NULL,
                    environment VARCHAR(20) NOT NULL,  -- PROD, DEV, etc.
                    database_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100) NOT NULL,
                    total_tests INTEGER NOT NULL,
                    passed_tests INTEGER NOT NULL,
                    overall_score DECIMAL(5,2) NOT NULL,
                    status VARCHAR(20) NOT NULL
                );
            """)
            
            # Create essential indexes for table summary metrics
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_summary_timestamp ON data_quality.summary_metrics_table (execution_timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_summary_environment ON data_quality.summary_metrics_table (environment);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_summary_database ON data_quality.summary_metrics_table (database_name);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_summary_table ON data_quality.summary_metrics_table (table_name);")
            
            # 4. Database-Level Summary Metrics - Aggregated metrics per database per environment
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality.summary_metrics_database (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(50) NOT NULL,
                    execution_timestamp TIMESTAMP NOT NULL,
                    environment VARCHAR(20) NOT NULL,  -- PROD, DEV, etc.
                    database_name VARCHAR(100) NOT NULL,
                    total_tables INTEGER NOT NULL,
                    successful_tables INTEGER NOT NULL,
                    failed_tables INTEGER NOT NULL,
                    total_tests INTEGER NOT NULL,
                    passed_tests INTEGER NOT NULL,
                    overall_score DECIMAL(5,2) NOT NULL,
                    status VARCHAR(20) NOT NULL
                );
            """)
            
            # Create essential indexes for database summary metrics
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_db_summary_timestamp ON data_quality.summary_metrics_database (execution_timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_db_summary_environment ON data_quality.summary_metrics_database (environment);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_db_summary_database ON data_quality.summary_metrics_database (database_name);")
            
            # 5. Freshness Table - Track latest dates and row counts per environment
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality.freshness (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(50) NOT NULL,
                    execution_timestamp TIMESTAMP NOT NULL,
                    environment VARCHAR(20) NOT NULL,  -- PROD, DEV, etc.
                    database_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100) NOT NULL,
                    latest_date TIMESTAMP,
                    row_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_freshness_environment ON data_quality.freshness (environment);
                CREATE INDEX IF NOT EXISTS idx_freshness_database ON data_quality.freshness (database_name);
                CREATE INDEX IF NOT EXISTS idx_freshness_table ON data_quality.freshness (table_name);
                CREATE INDEX IF NOT EXISTS idx_freshness_timestamp ON data_quality.freshness (execution_timestamp);
            """)
            
            # 6. Row Count History Table - Track row counts over time for dynamic thresholds per environment
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality.row_count_history (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(50) NOT NULL,
                    execution_timestamp TIMESTAMP NOT NULL,
                    environment VARCHAR(20) NOT NULL,  -- PROD, DEV, etc.
                    database_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100) NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_row_count_history_environment ON data_quality.row_count_history (environment);
                CREATE INDEX IF NOT EXISTS idx_row_count_history_database_table ON data_quality.row_count_history (database_name, table_name);
                CREATE INDEX IF NOT EXISTS idx_row_count_history_timestamp ON data_quality.row_count_history (execution_timestamp);
            """)
            
            cursor.close()
            logger.info("Successfully created essential data quality tables")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            return False
    
    
    def store_summary_metrics(self, execution_id: str, results: List[Dict[str, Any]]) -> bool:
        """
        Store summary metrics in separate table-level and database-level tables
        
        Args:
            execution_id: Unique execution identifier
            results: List of test results
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Import calculate_test_statistics with absolute import
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from utils.helpers import calculate_test_statistics
            
            cursor = self.connection.cursor()
            execution_timestamp = datetime.now()
            
            # Group results by environment, database and table
            env_db_table_groups = {}
            for result in results:
                if result.get('status') == 'SUCCESS':
                    environment = result.get('environment', 'UNKNOWN')
                    db_name = result['database']
                    table_name = result['table']
                    
                    if environment not in env_db_table_groups:
                        env_db_table_groups[environment] = {}
                    
                    if db_name not in env_db_table_groups[environment]:
                        env_db_table_groups[environment][db_name] = {'tables': {}, 'all_results': []}
                    
                    if table_name not in env_db_table_groups[environment][db_name]['tables']:
                        env_db_table_groups[environment][db_name]['tables'][table_name] = []
                    
                    env_db_table_groups[environment][db_name]['tables'][table_name].append(result)
                    env_db_table_groups[environment][db_name]['all_results'].append(result)
            
            table_records = []
            database_records = []
            
            # Calculate table-level and database-level metrics per environment
            for environment, env_data in env_db_table_groups.items():
                for db_name, db_data in env_data.items():
                    successful_tables = 0
                    failed_tables = 0
                    
                    for table_name, table_results in db_data['tables'].items():
                        table_stats = calculate_test_statistics(table_results)
                        
                        total_tests = table_stats['total_custom_tests'] + table_stats['total_gx_tests']
                        passed_tests = table_stats['passed_custom_tests'] + table_stats['passed_gx_tests']
                        pass_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 100
                        overall_score = pass_rate
                        status = self._get_status_from_score(overall_score, table_name)
                        
                        # Count successful/failed tables for database summary
                        if status in ['PASS', 'LOW', 'MEDIUM']:
                            successful_tables += 1
                        else:
                            failed_tables += 1
                        
                        table_records.append((
                            execution_id, execution_timestamp, environment, db_name, table_name,
                            total_tests, passed_tests, overall_score, status
                        ))
                    
                    # Calculate database-level metrics
                    db_stats = calculate_test_statistics(db_data['all_results'])
                    total_tests = db_stats['total_custom_tests'] + db_stats['total_gx_tests']
                    passed_tests = db_stats['passed_custom_tests'] + db_stats['passed_gx_tests']
                    pass_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 100
                    overall_score = pass_rate
                    status = self._get_status_from_score(overall_score)  # Database level uses default thresholds
                    total_tables = successful_tables + failed_tables
                    
                    database_records.append((
                        execution_id, execution_timestamp, environment, db_name, total_tables,
                        successful_tables, failed_tables, total_tests, passed_tests,
                        overall_score, status
                    ))
            
            # Insert table-level metrics
            if table_records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO data_quality.summary_metrics_table 
                    (execution_id, execution_timestamp, environment, database_name, table_name,
                     total_tests, passed_tests, overall_score, status)
                    VALUES %s
                    """,
                    table_records
                )
            
            # Insert database-level metrics
            if database_records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO data_quality.summary_metrics_database 
                    (execution_id, execution_timestamp, environment, database_name, total_tables,
                     successful_tables, failed_tables, total_tests, passed_tests,
                     overall_score, status)
                    VALUES %s
                    """,
                    database_records
                )
            
            cursor.close()
            logger.info(f"Stored {len(table_records)} table metrics and {len(database_records)} database metrics")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store summary metrics: {str(e)}")
            return False
    
    def store_dimension_scores(self, execution_id: str, results: List[Dict[str, Any]]) -> bool:
        """
        Store dimension scores per table per database
        
        Args:
            execution_id: Unique execution identifier
            results: List of test results
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            execution_timestamp = datetime.now()
            
            # Group results by database, table, and dimension
            dimension_groups = {}
            for result in results:
                if result.get('status') == 'SUCCESS':
                    db_name = result['database']
                    table_name = result['table']
                    
                    # Process custom tests (directly from result, not nested under test_results)
                    for custom_test in result.get('custom_tests', []):
                        dimension = custom_test.get('dimension', 'unknown')
                        # Skip tests with unknown dimension
                        if dimension == 'unknown':
                            continue
                        environment = result.get('environment', 'UNKNOWN')
                        key = f"{environment}|{db_name}|{table_name}|{dimension}"
                        
                        if key not in dimension_groups:
                            dimension_groups[key] = {
                                'environment': result.get('environment', 'UNKNOWN'),
                                'database_name': db_name,
                                'table_name': table_name,
                                'test_dimension': dimension,
                                'total_tests': 0,
                                'passed_tests': 0,
                                'failed_tests': 0
                            }
                        
                        dimension_groups[key]['total_tests'] += 1
                        if custom_test.get('passed', False):
                            dimension_groups[key]['passed_tests'] += 1
                        else:
                            dimension_groups[key]['failed_tests'] += 1
                    
                    # Process Great Expectations tests (directly from result)
                    for gx_test in result.get('great_expectations_tests', []):
                        # Map GX expectation types to dimensions
                        expectation_type = gx_test.get('expectation_type', '')
                        dimension = self._map_gx_to_dimension(expectation_type)
                        environment = result.get('environment', 'UNKNOWN')
                        key = f"{environment}|{db_name}|{table_name}|{dimension}"
                        
                        if key not in dimension_groups:
                            dimension_groups[key] = {
                                'environment': result.get('environment', 'UNKNOWN'),
                                'database_name': db_name,
                                'table_name': table_name,
                                'test_dimension': dimension,
                                'total_tests': 0,
                                'passed_tests': 0,
                                'failed_tests': 0
                            }
                        
                        dimension_groups[key]['total_tests'] += 1
                        if gx_test.get('success', False):
                            dimension_groups[key]['passed_tests'] += 1
                        else:
                            dimension_groups[key]['failed_tests'] += 1
            
            # Calculate scores and prepare records
            dimension_records = []
            
            for key, group_data in dimension_groups.items():
                total_tests = group_data['total_tests']
                passed_tests = group_data['passed_tests']
                failed_tests = group_data['failed_tests']
                
                dimension_score = (passed_tests / total_tests * 100) if total_tests > 0 else 0
                status = self._get_status_from_score(dimension_score)
                
                dimension_records.append((
                    execution_id, execution_timestamp, group_data['environment'],
                    group_data['database_name'], group_data['table_name'], group_data['test_dimension'],
                    total_tests, passed_tests, failed_tests,
                    dimension_score, status
                ))
            
            
            # Bulk insert dimension scores
            if dimension_records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO data_quality.dimension_scores 
                    (execution_id, execution_timestamp, environment, database_name, table_name, test_dimension,
                     total_tests, passed_tests, failed_tests, dimension_score, status)
                    VALUES %s
                    """,
                    dimension_records
                )
            
            cursor.close()
            logger.info(f"Stored {len(dimension_records)} dimension scores")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store dimension scores: {str(e)}")
            return False
    
    def _map_gx_to_dimension(self, expectation_type: str) -> str:
        """Map Great Expectations expectation types to data quality dimensions"""
        expectation_type = expectation_type.lower()
        
        if any(keyword in expectation_type for keyword in ['null', 'not_null', 'complete']):
            return 'completeness'
        elif any(keyword in expectation_type for keyword in ['unique', 'distinct']):
            return 'uniqueness'
        elif any(keyword in expectation_type for keyword in ['range', 'between', 'min', 'max', 'values']):
            return 'validity'
        elif any(keyword in expectation_type for keyword in ['format', 'regex', 'match']):
            return 'consistency'
        elif any(keyword in expectation_type for keyword in ['date', 'time']):
            return 'timeliness'
        else:
            return 'validity'  # Default dimension
    
    def store_freshness_data(self, execution_id: str, results: List[Dict[str, Any]]) -> bool:
        """
        Store freshness data (latest date and row count) for each table
        
        Args:
            execution_id: Unique execution identifier
            results: List of test results
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            execution_timestamp = datetime.now()
            
            # Process each result to extract freshness data
            freshness_records = []
            
            for result in results:
                if result.get('status') != 'SUCCESS':
                    continue
                    
                database = result.get('database')
                table = result.get('table')
                row_count = result.get('table_info', {}).get('row_count')
                latest_date = None
                
                # Find the latest date from timeliness checks
                for test in result.get('custom_tests', []):
                    if test.get('test_name') == 'date_insertion_freshness_check' and 'latest_datetime_found' in test:
                        try:
                            latest_date = datetime.strptime(
                                test['latest_datetime_found'], 
                                '%Y-%m-%d %H:%M:%S'
                            )
                        except (ValueError, TypeError):
                            try:
                                latest_date = datetime.strptime(
                                    test['latest_datetime_found'], 
                                    '%Y-%m-%d'
                                )
                            except (ValueError, TypeError):
                                pass
                        break
                
                if database and table and (latest_date or row_count is not None):
                    freshness_records.append((
                        execution_id,
                        execution_timestamp,
                        result.get('environment', 'UNKNOWN'),
                        database,
                        table,
                        latest_date,
                        row_count
                    ))
            
            # Batch insert freshness data
            if freshness_records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO data_quality.freshness 
                    (execution_id, execution_timestamp, environment, database_name, table_name, latest_date, row_count)
                    VALUES %s
                    """,
                    freshness_records
                )
                
                logger.info(f"Stored freshness data for {len(freshness_records)} tables")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error storing freshness data: {str(e)}")
            return False
            
    def _get_status_from_score(self, score: float, table_name: str = None) -> str:
        """Convert numeric score to urgency status string based on configurable thresholds"""
        from config import URGENCY_CONFIG
        
        # Get thresholds for this specific table, or use defaults
        if table_name and table_name in URGENCY_CONFIG.get('table_specific', {}):
            thresholds = URGENCY_CONFIG['table_specific'][table_name]
        else:
            thresholds = URGENCY_CONFIG['default_thresholds']
        
        # Determine urgency level based on thresholds
        if score < thresholds['critical']:
            return 'CRITICAL'
        elif score < thresholds['high']:
            return 'HIGH'
        elif score < thresholds['medium']:
            return 'MEDIUM'
        elif score < thresholds['low']:
            return 'LOW'
        else:
            return 'PASS'
    
    def store_row_count_history(self, execution_id: str, results: List[Dict[str, Any]]) -> bool:
        """
        Store row count history for dynamic threshold calculations
        
        Args:
            execution_id: Unique execution identifier
            results: List of test results containing row counts
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            execution_timestamp = datetime.now()
            
            # Extract row counts from results
            row_count_records = []
            for result in results:
                if result.get('status') == 'SUCCESS':
                    database = result.get('database')
                    table = result.get('table')
                    
                    # Look for row count in the result itself first
                    row_count = result.get('table_info', {}).get('row_count')
                    
                    # If not found, check in the tests
                    if row_count is None and 'tests' in result:
                        for test in result['tests']:
                            if test.get('test_name') == 'row_count_check' and 'actual_row_count' in test:
                                row_count = test.get('actual_row_count')
                                break
                    
                    if database and table and row_count is not None:
                        row_count_records.append((
                            execution_id,
                            execution_timestamp,
                            result.get('environment', 'UNKNOWN'),
                            database,
                            table,
                            row_count
                        ))
                        logger.info(f"Found row count for {database}.{table}: {row_count}")
                    else:
                        logger.warning(f"No row count found for {database}.{table} - checking structure: {list(result.keys())}")
            
            # Batch insert row count history
            if row_count_records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO data_quality.row_count_history 
                    (execution_id, execution_timestamp, environment, database_name, table_name, row_count)
                    VALUES %s
                    """,
                    row_count_records
                )
                
                logger.info(f"Stored row count history for {len(row_count_records)} tables")
                cursor.close()
                return True
            
            cursor.close()
            return False
            
        except Exception as e:
            logger.error(f"Error storing row count history: {str(e)}")
            return False

    def get_historical_row_counts(self, environment: str, database: str, table: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get historical row counts for a table from dedicated history table
        
        Args:
            environment: Environment name (PROD, DEV, etc.)
            database: Database name
            table: Table name  
            limit: Number of historical records to retrieve
            
        Returns:
            List of historical row count records ordered by timestamp (newest first)
        """
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT execution_timestamp, row_count
                FROM data_quality.row_count_history 
                WHERE environment = %s AND database_name = %s AND table_name = %s 
                ORDER BY execution_timestamp DESC
                LIMIT %s
            """, (environment, database, table, limit))
            
            results = cursor.fetchall()
            cursor.close()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error retrieving historical row counts for {environment}.{database}.{table}: {str(e)}")
            return []
