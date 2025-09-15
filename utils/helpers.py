"""
Utility functions for the Data Quality Framework
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

def save_results_to_json(results: List[Dict[str, Any]], 
                        output_dir: str = 'results', 
                        filename: Optional[str] = None) -> Optional[str]:
    """
    Save results to JSON file
    
    Args:
        results (List[Dict[str, Any]]): Test results
        output_dir (str): Output directory
        filename (str, optional): Output filename
        
    Returns:
        str: Path to saved file or None if failed
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp if not specified
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_quality_results_{timestamp}.json"
        
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Results saved to {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to save results: {str(e)}")
        return None

def setup_logging(level: str = 'INFO', format_str: Optional[str] = None) -> None:
    """
    Setup logging configuration
    
    Args:
        level (str): Logging level
        format_str (str, optional): Log format string
    """
    if format_str is None:
        format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_str
    )

def validate_config(config: Dict[str, Any], required_keys: List[str]) -> bool:
    """
    Validate configuration dictionary
    
    Args:
        config (Dict[str, Any]): Configuration to validate
        required_keys (List[str]): Required configuration keys
        
    Returns:
        bool: True if valid, False otherwise
    """
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        logger.error(f"Missing required configuration keys: {missing_keys}")
        return False
    return True

def create_directory_structure(base_dir: str, subdirs: List[str]) -> None:
    """
    Create directory structure
    
    Args:
        base_dir (str): Base directory path
        subdirs (List[str]): Subdirectories to create
    """
    try:
        os.makedirs(base_dir, exist_ok=True)
        for subdir in subdirs:
            os.makedirs(os.path.join(base_dir, subdir), exist_ok=True)
        logger.info(f"Created directory structure in {base_dir}")
    except Exception as e:
        logger.error(f"Failed to create directory structure: {str(e)}")

def format_row_count(count: int) -> str:
    """
    Format row count with thousands separator
    
    Args:
        count (int): Row count
        
    Returns:
        str: Formatted count
    """
    return f"{count:,}"

def calculate_test_statistics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate overall test statistics
    
    Args:
        results (List[Dict[str, Any]]): Test results
        
    Returns:
        Dict[str, Any]: Statistics summary
    """
    stats = {
        'total_tables': len(results),
        'successful_tables': 0,
        'failed_tables': 0,
        'total_custom_tests': 0,
        'passed_custom_tests': 0,
        'total_gx_tests': 0,
        'passed_gx_tests': 0,
        'test_types': {}
    }
    
    for result in results:
        if result['status'] == 'SUCCESS':
            stats['successful_tables'] += 1
            
            # Count custom tests
            for test in result.get('custom_tests', []):
                if test:
                    stats['total_custom_tests'] += 1
                    if test.get('passed', False):
                        stats['passed_custom_tests'] += 1
                    
                    # Count by test type
                    test_type = test.get('test_name', 'unknown')
                    if test_type not in stats['test_types']:
                        stats['test_types'][test_type] = {'total': 0, 'passed': 0}
                    stats['test_types'][test_type]['total'] += 1
                    if test.get('passed', False):
                        stats['test_types'][test_type]['passed'] += 1
            
            # Count Great Expectations tests
            for test in result.get('great_expectations_tests', []):
                stats['total_gx_tests'] += 1
                if test.get('success', False):
                    stats['passed_gx_tests'] += 1
        else:
            stats['failed_tables'] += 1
    
    return stats

def merge_results(results_list: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Merge multiple result lists
    
    Args:
        results_list (List[List[Dict[str, Any]]]): List of result lists
        
    Returns:
        List[Dict[str, Any]]: Merged results
    """
    merged = []
    for results in results_list:
        merged.extend(results)
    return merged

def filter_results(results: List[Dict[str, Any]], 
                  database: Optional[str] = None,
                  table: Optional[str] = None,
                  status: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Filter results based on criteria
    
    Args:
        results (List[Dict[str, Any]]): Results to filter
        database (str, optional): Filter by database
        table (str, optional): Filter by table
        status (str, optional): Filter by status
        
    Returns:
        List[Dict[str, Any]]: Filtered results
    """
    filtered = results
    
    if database:
        filtered = [r for r in filtered if r.get('database') == database]
    
    if table:
        filtered = [r for r in filtered if r.get('table') == table]
    
    if status:
        filtered = [r for r in filtered if r.get('status') == status]
    
    return filtered
