"""
Main entry point for the Data Quality Framework
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import great_expectations as gx

# Add the current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    HIVE_ENVIRONMENTS, get_all_environment_database_table_combinations,
    DATABASE_TABLE_CONFIG, get_all_database_table_combinations,
    get_tables_for_database, get_databases_for_table, POSTGRES_CONFIG, EMAIL_CONFIG, 
    QUALITY_CHECKS, ROW_COUNT_COMPARISON, OUTPUT_CONFIG, LOGGING_CONFIG, GX_CONFIG, get_schema
)
from core import HiveConnectionManager, DataFetcher, QualityChecker
from reporting import PDFReportGenerator, ConsoleReporter
from storage import PostgreSQLStorage
from utils import save_results_to_json, setup_logging, create_directory_structure, DataQualityEmailNotifier

logger = logging.getLogger(__name__)

class DataQualityFramework:
    """Main orchestrator for data quality checks"""
    
    def __init__(self):
        """Initialize the data quality framework"""
        # Setup logging
        setup_logging(
            level=LOGGING_CONFIG.get('level', 'INFO'),
            format_str=LOGGING_CONFIG.get('format')
        )
        
        # Initialize components with multi-environment support
        self.connection_manager = HiveConnectionManager(HIVE_ENVIRONMENTS)
        self.data_fetcher = None
        self.quality_checker = None
        self.pdf_generator = PDFReportGenerator()
        self.console_reporter = ConsoleReporter()
        
        # Initialize email notifier if enabled
        self.email_notifier = None
        if EMAIL_CONFIG.get('enabled', False):
            try:
                self.email_notifier = DataQualityEmailNotifier(EMAIL_CONFIG['smtp'])
                logger.info("Email notifier initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize email notifier: {str(e)}")
        
        # Initialize PostgreSQL storage if enabled
        self.postgres_storage = None
        if POSTGRES_CONFIG.get('enabled', False):
            try:
                self.postgres_storage = PostgreSQLStorage(POSTGRES_CONFIG['connection'])
                logger.info("PostgreSQL storage initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize PostgreSQL storage: {str(e)}")
        
        # Results storage
        self.results = []
        # Initialize row counts for all configured databases dynamically
        self.row_counts = {}
        for database in DATABASE_TABLE_CONFIG.keys():
            self.row_counts[database] = {}
        
        # Great Expectations context
        self.gx_context = None
        
    def setup_great_expectations(self) -> bool:
        """
        Initialize Great Expectations context
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create GX directory if it doesn't exist
            gx_dir = GX_CONFIG.get('context_root_dir', 'gx_temp')
            os.makedirs(gx_dir, exist_ok=True)
            
            # Initialize context
            self.gx_context = gx.get_context(context_root_dir=gx_dir)
            
            # Add pandas datasource
            datasource_config = {
                "name": GX_CONFIG['datasource_name'],
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    GX_CONFIG['data_connector_name']: {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name", "partition_id"]
                    }
                }
            }
            
            try:
                self.gx_context.add_datasource(**datasource_config)
            except Exception as e:
                logger.warning(f"Datasource may already exist: {str(e)}")
            
            logger.info("Great Expectations context initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations context: {str(e)}")
            return False
    
    def process_single_table(self, environment: str, database: str, table: str, expected_schema: Dict[str, str],
                            partition: Optional[Dict[str, str]] = None) -> bool:
        """
        Process a single table in specific environment (or partition if specified)
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            database (str): Database name
            table (str): Table name
            expected_schema (Dict[str, str]): Expected schema
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            bool: True if successful, False otherwise
        """
        part_str = f"partition {partition}" if partition else "full table"
        logger.info(f"Processing {environment}.{database}.{table} {part_str}")
        
        # Get table data for this partition from specific environment
        df = self.data_fetcher.fetch_data(environment, database, table, partition=partition)
        
        # Store row count for later environment comparison
        env_key = f"{environment}.{database}"
        if env_key not in self.row_counts:
            self.row_counts[env_key] = {}
        if df is not None:
            self.row_counts[env_key][table] = self.row_counts[env_key].get(table, 0) + len(df)
        
        # Get actual table schema (only once per table, not per partition)
        actual_schema = None
        if partition is None:  # Only get schema for non-partitioned tables or first partition
            actual_schema = self.data_fetcher.get_table_schema(environment, database, table)
            if not actual_schema:
                logger.error(f"Failed to get schema for {environment}.{database}.{table}")
                return False
        
        if df is None:
            # Record failed connection/query
            self.results.append({
                'database': database,
                'table': table,
                'partition': partition,
                'timestamp': datetime.now().isoformat(),
                'status': 'FAILED',
                'error': 'Failed to fetch table data',
                'quality_checks': [],
                'custom_tests': [],  # For backward compatibility
                'great_expectations_tests': []  # For backward compatibility
            })
            return False
        
        # Run all quality checks using Great Expectations as the primary engine
        quality_check_results = []
        if self.gx_context:
            quality_check_results = self.quality_checker.run_all_quality_checks(
                df, database, table, expected_schema, partition
            )
        else:
            logger.error(f"Great Expectations context not available for {database}.{table}")
            quality_check_results = [{
                'test_name': 'gx_context_error',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': 'Great Expectations context not available',
                'dimension': 'system',
                'error': 'Missing GX context'
            }]
        
        # Compile results
        table_result = {
            'environment': environment,  # Add environment information
            'database': database,
            'table': table,
            'partition': partition,
            'timestamp': datetime.now().isoformat(),
            'status': 'SUCCESS',
            'table_info': {
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': list(df.columns)
            },
            'schema_info': {
                'expected_schema': expected_schema,
                'actual_schema': actual_schema
            },
            'quality_checks': quality_check_results,
            # Maintain backward compatibility
            'custom_tests': quality_check_results,  # For backward compatibility with existing reports
            'great_expectations_tests': []  # Now empty as all tests are GX-based
        }
        
        self.results.append(table_result)
        logger.info(f"Completed processing {database}.{table} {part_str}")
        return True
    
    def run_environment_row_count_comparisons(self) -> None:
        """Compare row counts between PROD and DEV environments for same databases"""
        if not QUALITY_CHECKS.get('enable_row_count_comparison', True):
            return
            
        logger.info("Running row count comparisons between PROD and DEV environments...")
        logger.info(f"Available row counts: {self.row_counts}")
        
        # Get all databases and tables from DATABASE_TABLE_CONFIG
        for database, tables in DATABASE_TABLE_CONFIG.items():
            # Process each table in this database
            for table in tables:
                prod_key = f"PROD.{database}"
                dev_key = f"DEV.{database}"
                
                prod_count = self.row_counts.get(prod_key, {}).get(table, 0)
                dev_count = self.row_counts.get(dev_key, {}).get(table, 0)
                
                logger.info(f"Checking {database}.{table}: PROD={prod_count}, DEV={dev_count}")
                
                # Skip if we don't have data for both environments
                if dev_count == 0 or prod_count == 0:
                    logger.warning(f"Skipping row count comparison for {database}.{table} - missing data for one or both environments (PROD={prod_count}, DEV={dev_count})")
                    continue
                
                # Check if dev row count is less than or equal to prod row count
                passed = dev_count <= prod_count
                
                # Create a test result for the comparison
                comparison_result = {
                    'test_name': 'row_count_comparison',
                    'database': database,
                    'table': table,
                    'passed': passed,
                    'details': f"Environment row count comparison: DEV.{database}.{table}({dev_count}) <= PROD.{database}.{table}({prod_count})" if passed 
                             else f"Environment row count validation failed: DEV.{database}.{table}({dev_count}) > PROD.{database}.{table}({prod_count})",
                    'dev_row_count': dev_count,
                    'prod_row_count': prod_count,
                    'difference': dev_count - prod_count,  # Positive if dev > prod, negative if dev < prod
                    'timestamp': datetime.now().isoformat()
                }
                
                # Add the comparison result to all relevant test results for this table
                # We need to add it to both DEV and PROD results for this table
                added_count = 0
                matching_results = []
                for result in self.results:
                    if result['table'] == table and result['database'] == database:
                        matching_results.append(f"{result.get('environment', 'UNKNOWN')}")
                        if 'custom_tests' in result:
                            result['custom_tests'].append(comparison_result)
                            added_count += 1
                
                logger.info(f"Found {len(matching_results)} results for {database}.{table}: {matching_results}")
                logger.info(f"Added row count comparison for {database}.{table} to {added_count} result(s)")
    
    def run_all_checks(self, environment_database_table_combinations: Optional[List[tuple]] = None) -> bool:
        """
        Run all data quality checks for specified environment-database-table combinations
        
        Args:
            environment_database_table_combinations (List[tuple], optional): List of (environment, database, table) tuples to check
                                                                           (defaults to all configured combinations)
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Use all configured environment-database-table combinations if not specified
        if environment_database_table_combinations is None:
            environment_database_table_combinations = get_all_environment_database_table_combinations()
        
        logger.info(f"Starting data quality checks for {len(environment_database_table_combinations)} environment-database-table combinations")
        
        # Connect to all Hive environments
        connection_results = self.connection_manager.connect_all()
        if not any(connection_results.values()):
            logger.error("Failed to connect to any Hive environment")
            return False
        
        connected_envs = [env for env, connected in connection_results.items() if connected]
        failed_envs = [env for env, connected in connection_results.items() if not connected]
        
        logger.info(f"Connected to environments: {connected_envs}")
        if failed_envs:
            logger.warning(f"Failed to connect to environments: {failed_envs}")
            logger.warning("Row count comparisons will not be available if both DEV and PROD are not connected")
        
        # Initialize components
        self.data_fetcher = DataFetcher(self.connection_manager)
        
        # Setup Great Expectations - now mandatory for the framework
        if not self.setup_great_expectations():
            logger.error("Great Expectations setup failed - framework cannot continue without GX")
            return False
        
        # Initialize quality checker with Great Expectations context
        try:
            self.quality_checker = QualityChecker(self.gx_context)
        except ValueError as e:
            logger.error(f"Failed to initialize QualityChecker: {str(e)}")
            return False
        
        # Create output directories
        create_directory_structure('.', [
            OUTPUT_CONFIG.get('json_output_dir', 'results'),
            OUTPUT_CONFIG.get('pdf_output_dir', 'reports')
        ])
        
        # Process each valid environment-database-table combination
        for environment, database, table in environment_database_table_combinations:
            if not connection_results.get(environment, False):
                logger.warning(f"Skipping {environment}.{database}.{table} - no connection to {environment}")
                continue
                
            logger.info(f"Processing {environment}.{database}.{table}...")
            logger.debug(f"Environment being processed: '{environment}' (type: {type(environment)})")
            
            # Get expected schema for this table
            expected_schema = get_schema(table)
            if not expected_schema:
                logger.warning(f"No expected schema defined for table {table}")
                continue
            
            # Initialize row count for this environment.database.table
            env_key = f"{environment}.{database}"
            if env_key not in self.row_counts:
                self.row_counts[env_key] = {}
            self.row_counts[env_key][table] = 0
            
            # Check if table is partitioned
            partitions = self.data_fetcher.get_table_partitions(environment, database, table)
            
            if not partitions:
                # Process as non-partitioned table
                self.process_single_table(environment, database, table, expected_schema)
            else:
                # Process each partition
                logger.info(f"Found {len(partitions)} partitions for {environment}.{database}.{table}")
                for i, partition in enumerate(partitions, 1):
                    logger.info(f"Processing partition {i}/{len(partitions)}: {partition}")
                    self.process_single_table(environment, database, table, expected_schema, partition)
        
        # Run environment row count comparisons
        self.run_environment_row_count_comparisons()
        
        logger.info("Data quality checks completed")
        return True
    
    def generate_reports(self) -> Dict[str, Optional[str]]:
        """
        Generate all configured reports
        
        Returns:
            Dict[str, Optional[str]]: Paths to generated reports
        """
        report_paths = {}
        
        # Generate JSON report
        if OUTPUT_CONFIG.get('enable_json_output', True):
            json_path = save_results_to_json(
                self.results,
                OUTPUT_CONFIG.get('json_output_dir', 'results')
            )
            report_paths['json'] = json_path
        
        # Generate PDF report
        if OUTPUT_CONFIG.get('enable_pdf_output', True):
            pdf_path = self.pdf_generator.generate_report(
                self.results,
                OUTPUT_CONFIG.get('pdf_output_dir', 'reports')
            )
            report_paths['pdf'] = pdf_path
        
        return report_paths
    
    def send_email_notification(self, pdf_report_path: str) -> bool:
        """
        Send email notification with the data quality report
        
        Args:
            pdf_report_path (str): Path to the PDF report file
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            if not self.email_notifier:
                logger.warning("Email notifier not initialized")
                return False
            
            # Get email recipients from config
            recipients = EMAIL_CONFIG.get('recipients', {})
            to_emails = recipients.get('to', [])
            cc_emails = recipients.get('cc', [])
            bcc_emails = recipients.get('bcc', [])
            
            if not to_emails:
                logger.warning("No email recipients configured")
                return False
            
            # Test mode - only send to first recipient
            if EMAIL_CONFIG.get('test_mode', False):
                to_emails = [to_emails[0]] if to_emails else []
                cc_emails = []
                bcc_emails = []
                logger.info("Email test mode enabled - sending only to first recipient")
            
            # Send the email
            success = self.email_notifier.send_report(
                results=self.results,
                report_file_path=pdf_report_path,
                recipients=to_emails,
                cc_recipients=cc_emails if cc_emails else None,
                bcc_recipients=bcc_emails if bcc_emails else None
            )
            
            if success:
                self.console_reporter.print_success(f"📧 Email notification sent to {len(to_emails)} recipient(s)")
            else:
                self.console_reporter.print_error("❌ Failed to send email notification")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
            self.console_reporter.print_error(f"Email notification error: {str(e)}")
            return False
    
    def store_results_in_postgres(self) -> bool:
        """
        Store test results and metrics in PostgreSQL
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.postgres_storage:
                logger.warning("PostgreSQL storage not initialized")
                return False
            
            # Connect to PostgreSQL
            if not self.postgres_storage.connect():
                self.console_reporter.print_error("❌ Failed to connect to PostgreSQL")
                return False
            
            # Create tables if auto_create_tables is enabled
            if POSTGRES_CONFIG.get('auto_create_tables', True):
                if not self.postgres_storage.create_tables():
                    self.console_reporter.print_error("❌ Failed to create PostgreSQL tables")
                    return False
            
            # Generate unique execution ID
            execution_id = f"dq_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Execution tracking simplified (removed execution_log table)
            
            
            # Store summary metrics
            if not self.postgres_storage.store_summary_metrics(execution_id, self.results):
                self.console_reporter.print_error("❌ Failed to store summary metrics")
                return False
            
            # Store dimension scores
            if not self.postgres_storage.store_dimension_scores(execution_id, self.results):
                self.console_reporter.print_error("❌ Failed to store dimension scores")
                return False
            
            # Store freshness data
            if not self.postgres_storage.store_freshness_data(execution_id, self.results):
                self.console_reporter.print_warning("⚠️  No freshness data to store or failed to store freshness data")
            
            # Store row count history for dynamic threshold calculations
            self.console_reporter.print_info("📊 Storing row count history...")
            if not self.postgres_storage.store_row_count_history(execution_id, self.results):
                self.console_reporter.print_warning("⚠️  No row count history to store or failed to store row count history")
            else:
                self.console_reporter.print_success("✅ Row count history stored successfully")
            
            # Disconnect
            self.postgres_storage.disconnect()
            
            self.console_reporter.print_success(f"📊 Results stored in PostgreSQL (execution_id: {execution_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error storing results in PostgreSQL: {str(e)}")
            self.console_reporter.print_error(f"PostgreSQL storage error: {str(e)}")
            return False
    
    def run(self, databases: Optional[List[str]] = None, 
           tables: Optional[List[str]] = None) -> bool:
        """
        Run the complete data quality framework
        
        Args:
            databases (List[str], optional): Databases to check
            tables (List[str], optional): Tables to check
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert databases/tables parameters to database_table_combinations format
            if databases is not None or tables is not None:
                # If specific databases or tables are specified, use them
                target_databases = databases or list(DATABASE_TABLE_CONFIG.keys())
                target_tables = tables or []
                
                # Create all combinations of the specified databases and tables
                database_table_combinations = []
                for db in target_databases:
                    # If specific tables are provided, use those, otherwise use all tables for the database
                    db_tables = target_tables if target_tables else get_tables_for_database(db)
                    for table in db_tables:
                        database_table_combinations.append((db, table))
            else:
                # If no specific databases or tables are specified, use all configured combinations
                database_table_combinations = None
            
            # Run all checks with the converted parameters
            success = self.run_all_checks(database_table_combinations)
            
            if success:
                # Print console summary if enabled
                if OUTPUT_CONFIG.get('enable_console_output', True):
                    self.console_reporter.print_summary(self.results)
                
                # Generate reports
                report_paths = self.generate_reports()
                
                # Print report generation status
                for report_type, path in report_paths.items():
                    if path:
                        self.console_reporter.print_success(f"{report_type.upper()} report generated: {path}")
                    else:
                        self.console_reporter.print_error(f"Failed to generate {report_type.upper()} report")
                
                # Store results in PostgreSQL if enabled
                if self.postgres_storage:
                    self.store_results_in_postgres()
                
                # Send email notification if enabled and PDF report was generated
                if self.email_notifier and report_paths.get('pdf'):
                    self.send_email_notification(report_paths['pdf'])
                
                return True
            else:
                self.console_reporter.print_error("Failed to run data quality checks")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error in framework execution: {str(e)}")
            self.console_reporter.print_error(f"Unexpected error: {str(e)}")
            return False
        
        finally:
            # Clean up
            if self.connection_manager:
                self.connection_manager.disconnect_all()

def main():
    """Main entry point"""
    framework = DataQualityFramework()
    success = framework.run()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
