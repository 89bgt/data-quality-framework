"""
Console reporting module for data quality results
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class ConsoleReporter:
    """Handles console output formatting for data quality results"""
    
    def __init__(self):
        """Initialize console reporter"""
        pass
    
    def print_summary(self, results: List[Dict[str, Any]]) -> None:
        """
        Print a comprehensive summary of the test results
        
        Args:
            results (List[Dict[str, Any]]): Test results
        """
        print("\n" + "="*100)
        print("DATA QUALITY CHECK SUMMARY")
        print("="*100)
        
        total_tables = len(results)
        successful_tables = len([r for r in results if r['status'] == 'SUCCESS'])
        
        print(f"Total tables checked: {total_tables}")
        print(f"Successfully processed: {successful_tables}")
        print(f"Failed to process: {total_tables - successful_tables}")
        
        for result in results:
            print(f"\n{'='*50}")
            print(f"TABLE: {result['database']}.{result['table']}")
            if result.get('partition'):
                print(f"PARTITION: {result['partition']}")
            print(f"{'='*50}")
            print(f"Status: {result['status']}")
            print(f"Timestamp: {result['timestamp']}")
            
            if result['status'] == 'SUCCESS':
                print(f"Rows: {result['table_info']['row_count']:,}")
                print(f"Columns: {result['table_info']['column_count']}")
                
                print(f"\n--- DATA QUALITY RESULTS BY DIMENSION ---")
                
                # Organize tests by dimension
                dimension_tests = {
                    'completeness': [],
                    'consistency': [],
                    'timeliness': [],
                    'uniqueness': []
                }
                
                for test in result['custom_tests']:
                    if test:  # Skip None tests
                        dimension = test.get('dimension', 'other')
                        if dimension in dimension_tests:
                            dimension_tests[dimension].append(test)
                
                # Display results by dimension
                for dimension, tests in dimension_tests.items():
                    if tests:
                        print(f"\n  [DATA] {dimension.upper()} DIMENSION:")
                        for test in tests:
                            self._print_test_result(test, indent="    ")
                
                # Great Expectations results summary
                if result.get('great_expectations_tests'):
                    self._print_gx_results(result['great_expectations_tests'])
                
                # Schema comparison summary
                if 'schema_info' in result:
                    self._print_schema_summary(result['schema_info'])
                    
            else:
                print(f"Error: {result.get('error', 'Unknown error')}")
        
        # Overall summary statistics
        self._print_overall_statistics(results)
    
    def _print_test_result(self, test: Dict[str, Any], indent: str = "") -> None:
        """
        Print individual test result
        
        Args:
            test (Dict[str, Any]): Test result
            indent (str): Indentation for formatting
        """
        status = "✓ PASS" if test.get('passed', False) else "✗ FAIL"
        test_name = test.get('test_name', 'UNKNOWN_TEST').replace('_check', '').replace('_', ' ').title()
        print(f"{indent}{test_name}: {status}")
        print(f"{indent}  Details: {test.get('details', 'No details available')}")
        
        # Show detailed information for failed tests
        if not test.get('passed', False):
            self._print_failed_test_details(test, indent)
        
        # Always show schema presence details (missing/extra columns)
        if test.get('test_name') == 'schema_presence_check':
            self._print_schema_presence_details(test, indent)
        
        # Show row count comparison results
        elif test.get('test_name') == 'row_count_comparison':
            self._print_row_count_comparison_details(test, indent)
        
        # Show uniqueness test details
        elif test.get('test_name') == 'row_uniqueness_check':
            self._print_uniqueness_details(test, indent)
    
    def _print_failed_test_details(self, test: Dict[str, Any], indent: str = "") -> None:
        """
        Print details for failed tests
        
        Args:
            test (Dict[str, Any]): Failed test result
            indent (str): Indentation for formatting
        """
        test_name = test.get('test_name', '')
        
        if test_name == 'null_columns_check' and test.get('null_columns'):
            print(f"{indent}  Null-only columns: {test['null_columns']}")
        elif test_name == 'schema_types_check' and test.get('type_mismatches'):
            print(f"{indent}  Type mismatches ({len(test['type_mismatches'])}):")
            for mismatch in test['type_mismatches']:
                print(f"{indent}    - {mismatch['column']}: expected '{mismatch['expected_type']}', got '{mismatch['actual_type']}'")
    
    def _print_schema_presence_details(self, test: Dict[str, Any], indent: str = "") -> None:
        """
        Print schema presence check details
        
        Args:
            test (Dict[str, Any]): Schema presence test result
            indent (str): Indentation for formatting
        """
        missing_cols = test.get('missing_columns', [])
        extra_cols = test.get('extra_columns', [])
        
        if missing_cols:
            print(f"{indent}  Missing columns ({len(missing_cols)}): {missing_cols}")
        else:
            print(f"{indent}  Missing columns: None")
            
        if extra_cols:
            print(f"{indent}  Extra columns ({len(extra_cols)}): {extra_cols}")
        else:
            print(f"{indent}  Extra columns: None")
            
        print(f"{indent}  Expected: {test.get('expected_column_count', 0)} columns")
        print(f"{indent}  Actual: {test.get('actual_column_count', 0)} columns")
    
    def _print_row_count_comparison_details(self, test: Dict[str, Any], indent: str = "") -> None:
        """
        Print row count comparison details
        
        Args:
            test (Dict[str, Any]): Row count comparison test result
            indent (str): Indentation for formatting
        """
        if not test.get('passed', False):
            print(f"{indent}  DEV row count: {test.get('dev_row_count', 0):,}")
            print(f"{indent}  PROD row count: {test.get('prod_row_count', 0):,}")
            print(f"{indent}  Difference (PROD-DEV): {test.get('difference', 0):,}")
            print(f"{indent}  [ERROR] DEV row count should be less than PROD row count")
    
    def _print_uniqueness_details(self, test: Dict[str, Any], indent: str = "") -> None:
        """
        Print uniqueness test details
        
        Args:
            test (Dict[str, Any]): Uniqueness test result
            indent (str): Indentation for formatting
        """
        total_rows = test.get('total_rows', 0)
        unique_rows = test.get('unique_rows', 0)
        duplicate_rows = test.get('duplicate_rows', 0)
        duplicate_percentage = test.get('duplicate_percentage', 0.0)
        
        print(f"{indent}  Total rows: {total_rows:,}")
        print(f"{indent}  Unique rows: {unique_rows:,}")
        
        if duplicate_rows > 0:
            print(f"{indent}  [FAIL] Duplicate rows: {duplicate_rows:,} ({duplicate_percentage:.2f}%)")
        else:
            print(f"{indent}  [PASS] No duplicate rows found")
    
    def _print_gx_results(self, gx_tests: List[Dict[str, Any]]) -> None:
        """
        Print Great Expectations results
        
        Args:
            gx_tests (List[Dict[str, Any]]): Great Expectations test results
        """
        print(f"\n--- GREAT EXPECTATIONS RESULTS ---")
        gx_passed = len([t for t in gx_tests if t.get('success', False)])
        print(f"Tests passed: {gx_passed}/{len(gx_tests)}")
        
        # Show failed GX tests
        failed_gx = [t for t in gx_tests if not t.get('success', False)]
        if failed_gx:
            print("Failed tests:")
            for test in failed_gx:
                col_info = f" (column: {test['column']})" if test.get('column') else ""
                print(f"  - {test.get('expectation_type', 'unknown')}{col_info}")
    
    def _print_schema_summary(self, schema_info: Dict[str, Any]) -> None:
        """
        Print schema summary
        
        Args:
            schema_info (Dict[str, Any]): Schema information
        """
        expected_cols = len(schema_info.get('expected_schema', {}))
        actual_cols = len(schema_info.get('actual_schema', {}))
        print(f"\n--- SCHEMA SUMMARY ---")
        print(f"Expected columns: {expected_cols}")
        print(f"Actual columns: {actual_cols}")
    
    def _print_overall_statistics(self, results: List[Dict[str, Any]]) -> None:
        """
        Print overall test statistics
        
        Args:
            results (List[Dict[str, Any]]): All test results
        """
        print(f"\n{'='*100}")
        print("OVERALL TEST STATISTICS")
        print(f"{'='*100}")
        
        successful_tables = len([r for r in results if r['status'] == 'SUCCESS'])
        
        if successful_tables > 0:
            all_custom_tests = []
            all_gx_tests = []
            
            for result in results:
                if result['status'] == 'SUCCESS':
                    all_custom_tests.extend([t for t in result.get('custom_tests', []) if t])
                    all_gx_tests.extend(result.get('great_expectations_tests', []))
            
            # Custom tests summary
            if all_custom_tests:
                custom_passed = len([t for t in all_custom_tests if t.get('passed', False)])
                print(f"Custom Tests: {custom_passed}/{len(all_custom_tests)} passed")
                
                # Break down by test type
                test_types = {}
                for test in all_custom_tests:
                    test_type = test.get('test_name', 'unknown')
                    if test_type not in test_types:
                        test_types[test_type] = {'total': 0, 'passed': 0}
                    test_types[test_type]['total'] += 1
                    if test.get('passed', False):
                        test_types[test_type]['passed'] += 1
                
                for test_type, stats in test_types.items():
                    print(f"  {test_type}: {stats['passed']}/{stats['total']} passed")
            
            # Great Expectations summary
            if all_gx_tests:
                gx_passed = len([t for t in all_gx_tests if t.get('success', False)])
                print(f"Great Expectations Tests: {gx_passed}/{len(all_gx_tests)} passed")
        
        print(f"{'='*100}")
    
    def print_processing_status(self, database: str, table: str, partition: str = None) -> None:
        """
        Print processing status for a table/partition
        
        Args:
            database (str): Database name
            table (str): Table name
            partition (str, optional): Partition information
        """
        part_str = f" partition {partition}" if partition else ""
        print(f"Processing {database}.{table}{part_str}...")
    
    def print_completion_status(self, database: str, table: str, partition: str = None) -> None:
        """
        Print completion status for a table/partition
        
        Args:
            database (str): Database name
            table (str): Table name
            partition (str, optional): Partition information
        """
        part_str = f" partition {partition}" if partition else ""
        print(f"Completed processing {database}.{table}{part_str}")
    
    def print_error(self, message: str) -> None:
        """
        Print error message
        
        Args:
            message (str): Error message
        """
        print(f"[ERROR] {message}")
    
    def print_success(self, message: str) -> None:
        """
        Print success message
        
        Args:
            message (str): Success message
        """
        print(f"[SUCCESS] {message}")
    
    def print_warning(self, message: str) -> None:
        """
        Print warning message
        
        Args:
            message (str): Warning message
        """
        print(f"\n[WARNING] {message}")
        
    def print_info(self, message: str) -> None:
        """
        Print informational message
        
        Args:
            message (str): Informational message
        """
        print(f"\n[INFO] {message}")
