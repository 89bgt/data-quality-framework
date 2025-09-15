"""
Data quality check implementations using Great Expectations
"""

import pandas as pd
import os
import json
import logging
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.checkpoint import Checkpoint
from config import QUALITY_CHECKS, GX_CONFIG

logger = logging.getLogger(__name__)

class QualityChecker:
    """Implements data quality checks using Great Expectations as the primary engine"""
    
    def __init__(self, gx_context=None):
        """
        Initialize quality checker with Great Expectations context
        
        Args:
            gx_context: Great Expectations context (required)
        """
        if not gx_context:
            raise ValueError("Great Expectations context is required for QualityChecker")
        
        self.gx_context = gx_context
        
        # Data Quality Dimensions mapped to Great Expectations
        self.dimensions = {
            'completeness': ['expect_column_values_to_not_be_null', 'expect_table_row_count_to_be_between'],
            'consistency': ['expect_column_to_exist', 'expect_table_columns_to_match_set', 'expect_column_values_to_be_of_type'],
            'timeliness': ['expect_column_max_to_be_between'],
            'uniqueness': ['expect_table_row_count_to_equal', 'expect_compound_columns_to_be_unique']
        }
        
    def create_expectation_suite(self, database: str, table: str, expected_schema: Dict[str, str]) -> str:
        """
        Create or get an expectation suite for a table
        
        Args:
            database (str): Database name
            table (str): Table name
            expected_schema (Dict[str, str]): Expected schema
            
        Returns:
            str: Suite name
        """
        suite_name = f"{database}_{table}_suite"
        
        try:
            # Try to get existing suite
            suite = self.gx_context.get_expectation_suite(suite_name)
            logger.info(f"Using existing expectation suite: {suite_name}")
        except:
            # Create new suite using the correct method for EphemeralDataContext
            suite = self.gx_context.add_or_update_expectation_suite(suite_name)
            logger.info(f"Created new expectation suite: {suite_name}")
        
        return suite_name
    
    def get_validator(self, df: pd.DataFrame, database: str, table: str, 
                     partition: Optional[Dict[str, str]] = None):
        """
        Create a Great Expectations validator for the dataframe
        
        Args:
            df (pd.DataFrame): Table data
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Validator: Great Expectations validator
        """
        from great_expectations.core.batch import RuntimeBatchRequest
        
        suite_name = f"{database}_{table}_suite"
        
        # Create batch identifiers
        batch_identifiers = {
            "default_identifier_name": f"{database}_{table}",
            "partition_id": str(partition) if partition else "full_table"
        }
        
        # Create a batch request using RuntimeBatchRequest
        batch_request = RuntimeBatchRequest(
            datasource_name=GX_CONFIG['datasource_name'],
            data_connector_name=GX_CONFIG['data_connector_name'],
            data_asset_name=f"{database}_{table}",
            runtime_parameters={"batch_data": df},
            batch_identifiers=batch_identifiers
        )
        
        # Create or get expectation suite
        self.create_expectation_suite(database, table, {})
        
        # Create validator with batch request
        validator = self.gx_context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        return validator
    
    @staticmethod
    def normalize_hive_type(hive_type: str) -> str:
        """
        Normalize Hive data types for comparison with Great Expectations
        
        Args:
            hive_type (str): Hive data type
            
        Returns:
            str: Normalized type for Great Expectations
        """
        if not hive_type:
            return hive_type
            
        hive_type = str(hive_type).lower().strip()
        
        # First handle pandas/numpy dtypes (e.g., 'int64', 'float32')
        if 'int' in hive_type:
            return 'int'
        if 'float' in hive_type or 'double' in hive_type or 'decimal' in hive_type:
            return 'float'
        if 'bool' in hive_type:
            return 'bool'
        if 'date' in hive_type or 'time' in hive_type or 'stamp' in hive_type:
            return 'datetime'
            
        # Map Hive types to normalized types
        type_mappings = {
            # String types
            'varchar': 'string',
            'char': 'string', 
            'text': 'string',
            'string': 'string',
            
            # Numeric types (handled by prefix matching above, but keeping for completeness)
            'integer': 'int',
            'int': 'int',
            'long': 'int',
            'bigint': 'int',
            'smallint': 'int',
            'tinyint': 'int',
            'double': 'float',
            'real': 'float',
            'float': 'float',
            'decimal': 'float',
            'numeric': 'float',
            
            # Boolean
            'boolean': 'bool',
            'bool': 'bool',
            
            # Date/time
            'timestamp': 'datetime',
            'date': 'datetime',
            'datetime': 'datetime',
            
            # Other
            'binary': 'binary',
            'array': 'array',
            'map': 'map',
            'struct': 'struct'
        }
        
        # Return mapped type or default to string if not found
        for hive_t, gx_t in type_mappings.items():
            if hive_type.startswith(hive_t):
                return gx_t
                
        # Default to string for unknown types
        return 'string'
                
        return hive_type
    
    def check_schema_presence(self, df: pd.DataFrame, expected_schema: Dict[str, str], 
                            database: str, table: str, partition: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Check if all required columns are present using Great Expectations
        
        Args:
            df (pd.DataFrame): Table data
            expected_schema (Dict[str, str]): Expected table schema
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Dict[str, Any]: Test result
        """
        try:
            validator = self.get_validator(df, database, table, partition)
            
            # Check that expected columns exist
            missing_columns = []
            extra_columns = []
            
            expected_columns = set(col.lower() for col in expected_schema.keys())
            actual_columns = set(col.lower() for col in df.columns)
            
            missing_columns = list(expected_columns - actual_columns)
            extra_columns = list(actual_columns - expected_columns)
            
            # Use Great Expectations to validate column existence
            expectation_result = validator.expect_table_columns_to_match_set(
                column_set=list(expected_schema.keys()),
                exact_match=False  # Allow extra columns
            )
            
            passed = expectation_result.success and len(missing_columns) == 0
            
            return {
                'test_name': 'schema_presence_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': passed,
                'details': f"Missing columns: {len(missing_columns)}, Extra columns: {len(extra_columns)}",
                'dimension': 'consistency',
                'missing_columns': missing_columns,
                'extra_columns': extra_columns,
                'expected_column_count': len(expected_columns),
                'actual_column_count': len(actual_columns),
                'expectation_result': expectation_result.to_json_dict()
            }
            
        except Exception as e:
            logger.error(f"Schema presence check failed for {database}.{table}: {str(e)}")
            return {
                'test_name': 'schema_presence_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error checking schema presence: {str(e)}",
                'dimension': 'consistency',
                'error': str(e)
            }
    
    def check_schema_types(self, df: pd.DataFrame, expected_schema: Dict[str, str], 
                          database: str, table: str, partition: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Check if column data types match expected schema using Great Expectations
        
        Args:
            df (pd.DataFrame): Table data
            expected_schema (Dict[str, str]): Expected table schema
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Dict[str, Any]: Test result
        """
        try:
            validator = self.get_validator(df, database, table, partition)
            type_mismatches = []
            expectation_results = []
            
            for expected_col, expected_type in expected_schema.items():
                if expected_col in df.columns:
                    expected_type_normalized = self.normalize_hive_type(expected_type.lower())
                    
                    # Skip if column is empty (all nulls)
                    if df[expected_col].isna().all():
                        expectation_results.append({
                            'column': expected_col,
                            'expected_type': expected_type,
                            'expectation_result': {
                                'success': True,
                                'result': {'observed_value': 'all_null'}
                            }
                        })
                        continue
                        
                    # Map to pandas/Great Expectations type checking with improved type handling
                    if expected_type_normalized == 'int':
                        # For integers, check if values are whole numbers
                        if pd.api.types.is_numeric_dtype(df[expected_col]):
                            # Check if values are whole numbers
                            is_whole = df[expected_col].dropna().apply(lambda x: x == int(x) if pd.notnull(x) else True)
                            if is_whole.all():
                                expectation_result = validator.expect_column_to_exist(column=expected_col)
                                expectation_result.success = True
                            else:
                                expectation_result = validator.expect_column_values_to_be_of_type(
                                    column=expected_col, 
                                    type_='int',
                                    result_format='BASIC'
                                )
                        else:
                            expectation_result = validator.expect_column_values_to_be_of_type(
                                column=expected_col, 
                                type_='int',
                                result_format='BASIC'
                            )
                            
                    elif expected_type_normalized == 'float':
                        # For floats, check if values are numeric
                        if pd.api.types.is_numeric_dtype(df[expected_col]):
                            expectation_result = validator.expect_column_to_exist(column=expected_col)
                            expectation_result.success = True
                        else:
                            expectation_result = validator.expect_column_values_to_be_of_type(
                                column=expected_col, 
                                type_='float',
                                result_format='BASIC'
                            )
                            
                    elif expected_type_normalized == 'string':
                        # For strings, check if values are string-like
                        if pd.api.types.is_string_dtype(df[expected_col]) or pd.api.types.is_object_dtype(df[expected_col]):
                            expectation_result = validator.expect_column_to_exist(column=expected_col)
                            expectation_result.success = True
                        else:
                            expectation_result = validator.expect_column_values_to_be_of_type(
                                column=expected_col, 
                                type_='str',
                                result_format='BASIC'
                            )
                            
                    elif expected_type_normalized == 'bool':
                        # For booleans, check if values are boolean-like
                        if pd.api.types.is_bool_dtype(df[expected_col]) or \
                           (df[expected_col].dropna().isin([0, 1, True, False]).all()):
                            expectation_result = validator.expect_column_to_exist(column=expected_col)
                            expectation_result.success = True
                        else:
                            expectation_result = validator.expect_column_values_to_be_of_type(
                                column=expected_col, 
                                type_='bool',
                                result_format='BASIC'
                            )
                            
                    elif expected_type_normalized == 'datetime':
                        # For datetime, check multiple possible datetime types
                        if (pd.api.types.is_datetime64_any_dtype(df[expected_col]) or
                            pd.api.types.is_datetime64_dtype(df[expected_col]) or
                            pd.api.types.is_datetime64_ns_dtype(df[expected_col]) or
                            pd.api.types.is_datetime64tz_dtype(df[expected_col])):
                            expectation_result = validator.expect_column_to_exist(column=expected_col)
                            expectation_result.success = True
                        else:
                            # If not a datetime type, try to parse as string
                            try:
                                # First try to convert to datetime to see if it's parseable
                                pd.to_datetime(df[expected_col], errors='raise')
                                expectation_result = validator.expect_column_to_exist(column=expected_col)
                                expectation_result.success = True
                            except (ValueError, TypeError):
                                # If parsing fails, try the dateutil parseable check
                                expectation_result = validator.expect_column_values_to_be_dateutil_parseable(
                                    column=expected_col,
                                    result_format='BASIC'
                                )
                    else:
                        # For other types, just check that column exists
                        expectation_result = validator.expect_column_to_exist(column=expected_col)
                    
                    expectation_results.append({
                        'column': expected_col,
                        'expected_type': expected_type,
                        'expectation_result': expectation_result.to_json_dict()
                    })
                    
                    if not expectation_result.success:
                        actual_type = str(df[expected_col].dtype)
                        type_mismatches.append({
                            'column': expected_col,
                            'expected_type': expected_type,
                            'actual_type': actual_type,
                            'expected_normalized': expected_type_normalized,
                            'actual_normalized': actual_type
                        })
            
            passed = len(type_mismatches) == 0
            
            return {
                'test_name': 'schema_types_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': passed,
                'details': f"Type mismatches found: {len(type_mismatches)}",
                'dimension': 'consistency',
                'type_mismatches': type_mismatches,
                'total_columns_checked': len(expectation_results),
                'expectation_results': expectation_results
            }
            
        except Exception as e:
            logger.error(f"Schema types check failed for {database}.{table}: {str(e)}")
            return {
                'test_name': 'schema_types_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error checking schema types: {str(e)}",
                'dimension': 'consistency',
                'error': str(e)
            }
    
    def check_null_columns(self, df: pd.DataFrame, database: str, table: str, 
                          partition: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Check if any columns contain only NULL values using Great Expectations
        
        Args:
            df (pd.DataFrame): Table data
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Dict[str, Any]: Test result
        """
        try:
            total_rows = len(df)
            
            if total_rows == 0:
                return {
                    'test_name': 'null_columns_check',
                    'database': database,
                    'table': table,
                    'partition': partition,
                    'passed': True,  # No data means no null-only columns
                    'details': 'Table is empty - no null-only columns to check',
                    'dimension': 'completeness',
                    'null_columns': []
                }
            
            validator = self.get_validator(df, database, table, partition)
            null_columns = []
            expectation_results = []
            
            # Check each column for null values using Great Expectations
            for column in df.columns:
                # Expect at least one non-null value (mostly=0.01 means at least 1% non-null)
                expectation_result = validator.expect_column_values_to_not_be_null(
                    column=column,
                    mostly=0.01  # At least 1% of values should be non-null
                )
                
                expectation_results.append({
                    'column': column,
                    'expectation_result': expectation_result.to_json_dict()
                })
                
                # Check for all-null or all-empty string columns
                if not expectation_result.success:
                    # Check for all-null values
                    null_count = df[column].isnull().sum()
                    # Check for all-empty strings (for string columns)
                    empty_str_count = 0
                    if df[column].dtype == 'object':  # String/object type
                        empty_str_count = (df[column].astype(str).str.strip() == '').sum()
                    
                    if null_count == total_rows:  # All values are null
                        null_columns.append({
                            'column': column,
                            'issue': 'all_null',
                            'details': 'All values are NULL'
                        })
                    elif empty_str_count == total_rows:  # All values are empty strings
                        null_columns.append({
                            'column': column,
                            'issue': 'all_empty_strings',
                            'details': 'All values are empty strings'
                        })
                    elif null_count + empty_str_count == total_rows:  # All values are either null or empty strings
                        null_columns.append({
                            'column': column,
                            'issue': 'all_null_or_empty',
                            'details': f'All values are either NULL ({null_count} rows) or empty strings ({empty_str_count} rows)'
                        })
            
            passed = len(null_columns) == 0
            
            return {
                'test_name': 'null_columns_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': passed,
                'details': f"Found {len(null_columns)} columns with all NULL or empty values: {', '.join([col['column'] for col in null_columns])}" if not passed else "No columns with all NULL or empty values found",
                'dimension': 'completeness',
                'null_columns': null_columns,
                'total_rows': total_rows,
                'expectation_results': expectation_results
            }
            
        except Exception as e:
            logger.error(f"Null columns check failed for {database}.{table}: {str(e)}")
            return {
                'test_name': 'null_columns_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error checking null columns: {str(e)}",
                'dimension': 'completeness',
                'error': str(e)
            }
    
    def _calculate_dynamic_row_count_threshold(self, database: str, table: str, current_row_count: int, environment: str = 'DEV') -> Dict[str, Any]:
        """
        Calculate dynamic row count thresholds based on historical data
        Simple rule: <2 records = manual, >=2 records = average +/- percentage
        
        Args:
            database: Database name
            table: Table name
            current_row_count: Current row count
            
        Returns:
            Dict containing min_rows, max_rows, and calculation details
        """
        from config import QUALITY_CHECKS
        
        dynamic_config = QUALITY_CHECKS.get('completeness', {}).get('dynamic_row_count', {})
        
        logger.info(f"Dynamic config for {database}.{table}: {dynamic_config}")
        
        if not dynamic_config.get('enabled', False):
            logger.warning(f"Dynamic row count calculation is DISABLED for {database}.{table}")
            return {
                'min_rows': QUALITY_CHECKS.get('completeness', {}).get('row_count_minimum', 2),
                'max_rows': None,
                'calculation_method': 'manual'
            }
        
        logger.info(f"Dynamic row count calculation is ENABLED for {database}.{table}")
        
        # Get historical row counts
        try:
            from storage import PostgreSQLStorage
            from config import POSTGRES_CONFIG
            
            postgres_storage = PostgreSQLStorage(POSTGRES_CONFIG['connection'])
            if not postgres_storage.connect():
                return {
                    'min_rows': QUALITY_CHECKS.get('completeness', {}).get('row_count_minimum', 2),
                    'max_rows': None,
                    'calculation_method': 'manual'
                }
            
            # Get historical row counts from dedicated table
            historical_data = postgres_storage.get_historical_row_counts(environment, database, table, 10)
            postgres_storage.disconnect()
            
            logger.info(f"Dynamic calculation for {database}.{table}: found {len(historical_data)} historical records")
            logger.info(f"Historical data: {historical_data}")
            
            # Simple rule: less than 2 records = manual
            if len(historical_data) < 2:
                logger.info(f"Using manual method for {database}.{table} - insufficient history ({len(historical_data)} records)")
                return {
                    'min_rows': QUALITY_CHECKS.get('completeness', {}).get('row_count_minimum', 2),
                    'max_rows': None,
                    'calculation_method': 'manual'
                }
            
            # 2 or more records = calculate average increment +/- percentage
            logger.info(f"Using increment method for {database}.{table} - sufficient history ({len(historical_data)} records)")
            row_counts = [record['row_count'] for record in historical_data]
            row_counts.reverse()  # Oldest first for increment calculation
            
            logger.info(f"Row counts (oldest to newest): {row_counts}")
            
            # Calculate increments between consecutive executions
            increments = []
            for i in range(1, len(row_counts)):
                increment = row_counts[i] - row_counts[i-1]
                increments.append(increment)
                logger.info(f"Increment {i}: {row_counts[i-1]} -> {row_counts[i]} = +{increment}")
            
            # Calculate average increment
            average_increment = sum(increments) / len(increments) if increments else 0
            tolerance_pct = dynamic_config.get('tolerance_percentage', 20.0) / 100.0
            
            logger.info(f"Average increment: {average_increment}, tolerance: {tolerance_pct*100}%")
            
            # Use most recent row count as baseline
            latest_count = row_counts[-1]
            
            # Calculate expected increment range
            min_increment = average_increment * (1 - tolerance_pct)
            max_increment = average_increment * (1 + tolerance_pct)
            
            # Calculate expected row count range
            min_rows = int(latest_count + min_increment)
            max_rows = int(latest_count + max_increment)
            
            logger.info(f"Expected range: {min_rows} - {max_rows} (latest: {latest_count} + increment: {min_increment} to {max_increment})")
            
            return {
                'min_rows': max(min_rows, 1),  # Ensure at least 1 row
                'max_rows': max_rows,
                'calculation_method': 'increment',
                'average_increment': average_increment,
                'latest_count': latest_count,
                'historical_records': len(historical_data)
            }
            
        except Exception as e:
            logger.error(f"Dynamic calculation FAILED for {database}.{table}: {str(e)}")
            logger.error(f"Exception details: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                'min_rows': QUALITY_CHECKS.get('completeness', {}).get('row_count_minimum', 2),
                'max_rows': None,
                'calculation_method': 'manual'
            }

    def check_row_count(self, df: pd.DataFrame, database: str, table: str, 
                       min_rows: Optional[int] = None, partition: Optional[Dict[str, str]] = None,
                       environment: str = 'DEV') -> Dict[str, Any]:
        """
        Check if table contains minimum required rows using Great Expectations
        
        Args:
            df (pd.DataFrame): Table data
            database (str): Database name
            table (str): Table name
            min_rows (int, optional): Minimum required rows
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Dict[str, Any]: Test result
        """
        try:
            row_count = len(df)
            
            # Calculate dynamic thresholds if min_rows not explicitly provided
            if min_rows is None:
                logger.info(f"Calculating dynamic thresholds for {database}.{table}")
                threshold_info = self._calculate_dynamic_row_count_threshold(database, table, row_count, environment)
                min_rows = threshold_info['min_rows']
                max_rows = threshold_info.get('max_rows')
                calculation_method = threshold_info['calculation_method']
                logger.info(f"Dynamic calculation result: min={min_rows}, max={max_rows}, method={calculation_method}")
            else:
                max_rows = None
                calculation_method = 'manual'
                logger.info(f"Using explicit min_rows={min_rows} for {database}.{table}")
                
            validator = self.get_validator(df, database, table, partition)
            
            # Use Great Expectations to check minimum row count only
            expectation_result = validator.expect_table_row_count_to_be_between(
                min_value=min_rows,
                max_value=None
            )
            
            passed = expectation_result.success
            
            # Create simple details message - only show minimum threshold
            if calculation_method == 'increment':
                details = f"Table has {row_count} rows (minimum: {min_rows} based on increment trend)"
            else:
                details = f"Table has {row_count} rows (minimum: {min_rows})"
            
            return {
                'test_name': 'row_count_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': passed,
                'details': details,
                'dimension': 'completeness',
                'actual_row_count': row_count,
                'minimum_required': min_rows,
                'calculation_method': calculation_method,
                'expectation_result': expectation_result.to_json_dict()
            }
            
        except Exception as e:
            logger.error(f"Row count check failed for {database}.{table}: {str(e)}")
            return {
                'test_name': 'row_count_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error checking row count: {str(e)}",
                'dimension': 'completeness',
                'error': str(e)
            }
    
    def check_date_insertion_freshness(self, df: pd.DataFrame, database: str, table: str, 
                                      partition: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Check if the latest date_insertion equals yesterday's date using Great Expectations
        
        Args:
            df (pd.DataFrame): Table data
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Dict[str, Any]: Test result
        """
        date_column = 'date_insertion'
        
        # Check if date_insertion column exists
        if date_column not in df.columns:
            return {
                'test_name': 'date_insertion_freshness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Column '{date_column}' not found in table",
                'dimension': 'timeliness',
                'latest_date_found': None,
                'expected_date': None,
                'error': f"Missing column: {date_column}"
            }
        
        # Calculate yesterday's date
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        # Handle empty dataframe
        if len(df) == 0:
            return {
                'test_name': 'date_insertion_freshness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': "Table is empty - no date_insertion values to check",
                'dimension': 'timeliness',
                'latest_date_found': None,
                'expected_date': yesterday.strftime('%Y-%m-%d'),
                'error': "Empty table"
            }
        
        try:
            validator = self.get_validator(df, database, table, partition)
            
            # Convert date_insertion column to datetime
            date_series = pd.to_datetime(df[date_column], errors='coerce')
            valid_dates = date_series.dropna()
            
            if len(valid_dates) == 0:
                return {
                    'test_name': 'date_insertion_freshness_check',
                    'database': database,
                    'table': table,
                    'partition': partition,
                    'passed': False,
                    'details': f"No valid dates found in '{date_column}' column",
                    'dimension': 'timeliness',
                    'latest_date_found': None,
                    'expected_date': yesterday.strftime('%Y-%m-%d'),
                    'error': "No valid dates in column"
                }
            
            # Use Great Expectations to check if the maximum date equals yesterday
            expectation_result = validator.expect_column_max_to_be_between(
                column=date_column,
                min_value=yesterday,
                max_value=yesterday,
                parse_strings_as_datetimes=True
            )
            
            # Find the latest date for reporting
            latest_datetime = valid_dates.max()
            latest_date = latest_datetime.date()
            
            passed = expectation_result.success
            
            # Calculate difference for reporting
            if latest_date != yesterday:
                date_diff = (latest_date - yesterday).days
                if date_diff > 0:
                    diff_description = f"{date_diff} day(s) ahead of expected"
                else:
                    diff_description = f"{abs(date_diff)} day(s) behind expected"
            else:
                diff_description = "matches expected date"
            
            return {
                'test_name': 'date_insertion_freshness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': passed,
                'details': f"Latest date {latest_date.strftime('%Y-%m-%d')} {diff_description}",
                'dimension': 'timeliness',
                'latest_date_found': latest_date.strftime('%Y-%m-%d'),
                'latest_datetime_found': latest_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'expected_date': yesterday.strftime('%Y-%m-%d'),
                'test_execution_date': today.strftime('%Y-%m-%d'),
                'date_difference_days': (latest_date - yesterday).days if latest_date != yesterday else 0,
                'total_valid_dates': len(valid_dates),
                'total_records': len(df),
                'expectation_result': expectation_result.to_json_dict()
            }
            
        except Exception as e:
            logger.error(f"Date insertion freshness check failed for {database}.{table}: {str(e)}")
            return {
                'test_name': 'date_insertion_freshness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error processing date_insertion column: {str(e)}",
                'dimension': 'timeliness',
                'latest_date_found': None,
                'expected_date': yesterday.strftime('%Y-%m-%d'),
                'error': str(e)
            }
    
    def check_row_uniqueness(self, df: pd.DataFrame, database: str, table: str, 
                           partition: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Check for duplicate rows in the table using Great Expectations (Uniqueness dimension)
        
        Args:
            df (pd.DataFrame): Table data
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            Dict[str, Any]: Test result
        """
        total_rows = len(df)
        
        if total_rows == 0:
            return {
                'test_name': 'row_uniqueness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': True,
                'details': 'Table is empty - no duplicates to check',
                'dimension': 'uniqueness',
                'total_rows': 0,
                'unique_rows': 0,
                'duplicate_rows': 0,
                'duplicate_percentage': 0.0
            }
        
        try:
            validator = self.get_validator(df, database, table, partition)
            
            # Count unique rows
            unique_rows = len(df.drop_duplicates())
            duplicate_rows = total_rows - unique_rows
            duplicate_percentage = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0.0
            
            # Use Great Expectations to check that row count equals unique row count
            expectation_result = validator.expect_table_row_count_to_equal(
                value=unique_rows
            )
            
            # Alternative: Check if all rows are unique using compound columns
            if len(df.columns) > 0:
                compound_uniqueness_result = validator.expect_compound_columns_to_be_unique(
                    column_list=list(df.columns)
                )
            else:
                compound_uniqueness_result = None
            
            # Test passes if no duplicates found
            passed = duplicate_rows == 0 and expectation_result.success
            
            if passed:
                details = f"No duplicate rows found - all {total_rows:,} rows are unique"
            else:
                details = f"Found {duplicate_rows:,} duplicate rows ({duplicate_percentage:.2f}% of total)"
            
            return {
                'test_name': 'row_uniqueness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': passed,
                'details': details,
                'dimension': 'uniqueness',
                'total_rows': total_rows,
                'unique_rows': unique_rows,
                'duplicate_rows': duplicate_rows,
                'duplicate_percentage': duplicate_percentage,
                'expectation_result': expectation_result.to_json_dict(),
                'compound_uniqueness_result': compound_uniqueness_result.to_json_dict() if compound_uniqueness_result else None
            }
            
        except Exception as e:
            logger.error(f"Row uniqueness check failed for {database}.{table}: {str(e)}")
            return {
                'test_name': 'row_uniqueness_check',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error checking row uniqueness: {str(e)}",
                'dimension': 'uniqueness',
                'error': str(e),
                'total_rows': total_rows,
                'unique_rows': None,
                'duplicate_rows': None,
                'duplicate_percentage': None
            }
    
    def run_openmetadata_checkpoint(self, df: pd.DataFrame, database: str, table: str, 
                                  expectation_suite_name: str = None) -> Dict[str, Any]:
        """
        Store validation results in OpenMetadata using direct API calls.
        
        Args:
            df: DataFrame containing the data to validate
            database: Database name
            table: Table name
            expectation_suite_name: Not used in this implementation (kept for compatibility)
                                  
        Returns:
            Dict containing the operation result
        """
        try:
            import requests
            import json
            from datetime import datetime
            
            # Get OpenMetadata configuration
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'openmetadata.yaml')
            with open(config_path, 'r') as f:
                import yaml
                config = yaml.safe_load(f)
                
            # Extract configuration with defaults
            base_url = config.get('hostPort', 'http://your-openmetadata-host:8585/api').rstrip('/api')
            api_version = config.get('api_version', 'v1')
            
            # Get JWT token from security config
            jwt_token = config.get('securityConfig', {}).get('jwtToken', '').strip('"\'')
            
            if not jwt_token:
                raise ValueError("JWT token not found in OpenMetadata configuration")
            
            # Generate test case name in the format used by OpenMetadata
            test_case_name = f"dgi_hive.default.{database}.{table}.all.row_count_check_{database}_{table}"
            
            # Prepare test case data with supported fields only
            test_case_data = {
                "name": test_case_name,
                "entityLink": f"<#E::table::dgi_hive.default.{database}.{table}::columns::all>",  # Added 'default' schema
                "testDefinition": "tableRowCountToBeBetween",  # Using built-in test definition
                # Using the default admin user ID (replace with actual admin UUID if different)
                "owners": [
                    {
                        "id": "00000000-0000-0000-0000-000000000000",  # Default admin UUID
                        "type": "user"
                    }
                ],
                "parameterValues": [
                    {
                        "name": "minValue",
                        "value": 0
                    },
                    {
                        "name": "maxValue",
                        "value": 1000000  # Adjust based on your expected row count
                    }
                ],
                "computePassedFailedRowCount": True
            }
            
            # First, check if test case exists with the exact name
            test_case_name_encoded = requests.utils.quote(test_case_name, safe='')
            # Try with both the exact name and the short name
            possible_names = [
                test_case_name,
                f"row_count_check_{database}_{table}"  # Try the short name format
            ]
            
            # Also try with the format we see in the API response
            possible_names.append(f"row_count_check_{database}_{table}_{int(time.time())}")
            
            get_test_case_urls = [
                f"{base_url}/api/{api_version}/dataQuality/testCases/name/{requests.utils.quote(name, safe='')}" 
                for name in possible_names
            ]
            
            # Log the exact URLs being called
            logger.debug(f"Looking up test cases at: {get_test_case_urls}")
            
            # Set up headers
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {jwt_token}"
            }
            
            # Initialize test_case_id
            test_case_id = None
            
            # Try all possible test case names
            test_case_id = None
            response = None
            
            for url in get_test_case_urls:
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    logger.debug(f"Test case lookup response for {url}: {response.status_code}")
                    
                    if response.status_code == 200:
                        test_case = response.json()
                        test_case_id = test_case.get('id')
                        logger.info(f"Found existing test case: {test_case_id}")
                        break
                        
                except Exception as e:
                    logger.warning(f"Error checking test case at {url}: {str(e)}")
            
            if not test_case_id:
                # Test case doesn't exist, create it
                test_case_url = f"{base_url}/api/{api_version}/dataQuality/testCases"
                logger.info(f"Creating new test case at: {test_case_url}")
                logger.debug(f"Test case data: {json.dumps(test_case_data, indent=2)}")
                
                try:
                    response = requests.post(
                        test_case_url,
                        headers=headers,
                        json=test_case_data,
                        timeout=10
                    )
                    logger.debug(f"Create test case response: {response.status_code} - {response.text}")
                    
                    if response.status_code == 201:
                        test_case_id = response.json().get('id')
                        # Add delay to allow OpenMetadata to sync
                        logger.info("Test case created. Waiting 30 seconds for OpenMetadata to sync...")
                        time.sleep(30)
                    
                except Exception as e:
                    logger.error(f"Error creating test case: {str(e)}", exc_info=True)
                    return {
                        "status": "PARTIAL",
                        "error": f"Failed to create test case: {str(e)}",
                        "suggestion": "Check OpenMetadata server logs for more details"
                    }
                
                if response.status_code == 409:  # Handle race condition
                    # Try a few times with delay in case of eventual consistency
                    max_retries = 3
                    for attempt in range(max_retries):
                        time.sleep(1)  # Wait before retry
                        # Try each URL in sequence
                        for url in get_test_case_urls:
                            try:
                                response = requests.get(url, headers=headers, timeout=10)
                                if response.status_code == 200:
                                    test_case = response.json()
                                    test_case_id = test_case.get('id')
                                    logger.info(f"Found test case after race condition: {test_case_id}")
                                    break
                            except Exception as e:
                                logger.warning(f"Error during race condition retry: {str(e)}")
                        
                        if test_case_id:
                            break
                            
                        logger.warning(f"Attempt {attempt + 1}: Test case not found yet, retrying...")
                    
                    if not test_case_id:
                        error_msg = (
                            f"Test case '{test_case_name}' appears to exist but could not be retrieved. "
                            f"This could be due to permission issues or eventual consistency. "
                            f"Please check in OpenMetadata UI."
                        )
                        logger.error(error_msg)
                        return {
                            "status": "PARTIAL",
                            "error": error_msg,
                            "suggestion": "The test case might exist but couldn't be accessed. Check OpenMetadata UI."
                        }
                elif response.status_code == 201:
                    test_case_id = response.json().get('id')
                else:
                    error_msg = f"Failed to create test case: {response.text}"
                    logger.error(error_msg)
                    return {"status": "FAILED", "error": f"API error: {response.text}"}
            
            if not test_case_id:
                error_msg = "Test case ID could not be determined"
                logger.error(error_msg)
                return {"status": "FAILED", "error": error_msg}
            
            # Prepare test result data
            timestamp = int(datetime.now().timestamp() * 1000)  # Current time in milliseconds
            test_result_data = {
                "timestamp": timestamp,
                "testCaseResult": {
                    "testCaseStatus": "Success" if not df.empty else "Failed",
                    "result": f"Found {len(df)} rows",
                    "testResultValue": [
                        {
                            "value": len(df),
                            "name": "rowCount"
                        },
                        {
                            "value": 0 if not df.empty else 1,
                            "name": "failedCount"
                        }
                    ]
                }
            }
            
            # Submit test result
            result_url = f"{base_url}/api/{api_version}/dataQuality/testCases/{test_case_id}/testCaseResult"
            response = requests.put(
                result_url,
                headers=headers,
                json=test_result_data
            )
            
            if response.status_code not in (200, 201):
                logger.error(f"Failed to submit test result: {response.text}")
                return {
                    "status": "PARTIAL",
                    "error": f"Failed to submit result: {response.text}"
                }
            
            return {
                "status": "SUCCESS",
                "test_case_id": test_case_id,
                "row_count": len(df),
                "error": None
            }
            
        except Exception as e:
            logger.error(f"Error in OpenMetadata integration: {str(e)}", exc_info=True)
            return {
                "status": "FAILED",
                "error": str(e)
            }

    def run_all_quality_checks(self, df: pd.DataFrame, database: str, table: str, 
                              expected_schema: Dict[str, str], 
                              partition: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        Run all data quality checks using Great Expectations as the primary engine
        
        Args:
            df (pd.DataFrame): Table data
            database (str): Database name
            table (str): Table name
            expected_schema (Dict[str, str]): Expected schema
            partition (Dict[str, str], optional): Partition information
            
        Returns:
            List[Dict[str, Any]]: All quality check results
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {database}.{table}")
            return []
            
        try:
            # Create expectation suite for this table
            suite_name = self.create_expectation_suite(database, table, expected_schema)
            logger.info(f"Running quality checks for {database}.{table} using suite: {suite_name}")
            
            all_results = []
            
            # COMPLETENESS DIMENSION
            completeness_config = QUALITY_CHECKS.get('completeness', {})
            if completeness_config.get('enable_null_checks', True):
                result = self.check_null_columns(df, database, table, partition)
                all_results.append(result)
            
            if completeness_config.get('enable_row_count_check', True):
                # Don't pass min_rows to enable dynamic calculation
                result = self.check_row_count(df, database, table, None, partition)
                all_results.append(result)
            
            # CONSISTENCY DIMENSION
            consistency_config = QUALITY_CHECKS.get('consistency', {})
            if consistency_config.get('enable_schema_checks', True):
                if consistency_config.get('enable_schema_presence_check', True):
                    result = self.check_schema_presence(df, expected_schema, database, table, partition)
                    all_results.append(result)
                if consistency_config.get('enable_schema_types_check', True):
                    result = self.check_schema_types(df, expected_schema, database, table, partition)
                    all_results.append(result)
            
            # TIMELINESS DIMENSION
            timeliness_config = QUALITY_CHECKS.get('timeliness', {})
            if timeliness_config.get('enable_date_freshness', True):
                result = self.check_date_insertion_freshness(df, database, table, partition)
                all_results.append(result)
            
            # UNIQUENESS DIMENSION
            uniqueness_config = QUALITY_CHECKS.get('uniqueness', {})
            if uniqueness_config.get('enable_row_uniqueness_check', True):
                result = self.check_row_uniqueness(df, database, table, partition)
                all_results.append(result)
            
            # Run OpenMetadata checkpoint to store all results
            #try:
                #om_result = self.run_openmetadata_checkpoint(df, database, table)
                #if om_result["status"] == "SUCCESS":
                    #logger.info(f"Successfully stored results in OpenMetadata for {database}.{table}")
                #else:
                    #logger.error(f"Failed to store results in OpenMetadata: {om_result.get('error')}")
            #except Exception as e:
                #logger.error(f"Error during OpenMetadata checkpoint: {str(e)}")
            
            logger.info(f"Completed {len(all_results)} quality checks for {database}.{table}")
            return all_results
            
        except Exception as e:
            logger.error(f"Quality checks failed for {database}.{table}: {str(e)}")
            return [{
                'test_name': 'quality_checks_error',
                'database': database,
                'table': table,
                'partition': partition,
                'passed': False,
                'details': f"Error running quality checks: {str(e)}",
                'dimension': 'system',
                'error': str(e)
            }]
