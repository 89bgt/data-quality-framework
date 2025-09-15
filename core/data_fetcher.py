"""
Data fetching and partitioning module
"""

import pandas as pd
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

class DataFetcher:
    """Handles data fetching from Hive tables with partitioning support"""
    
    def __init__(self, connection_manager):
        """
        Initialize data fetcher
        
        Args:
            connection_manager: HiveConnectionManager instance (multi-environment)
        """
        self.connection_manager = connection_manager
        
    def get_table_partitions(self, environment: str, database: str, table: str) -> Optional[List[Dict[str, str]]]:
        """
        Get list of partitions for a table in specific environment
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            database (str): Database name
            table (str): Table name
            
        Returns:
            List[Dict[str, str]]: List of partition dictionaries or None if not partitioned
        """
        try:
            query = f"SHOW PARTITIONS {database}.{table}"
            results = self.connection_manager.execute_query(environment, query)
            
            if not results:
                return None
                
            # Parse partition information
            partitions = []
            for row in results:
                part_str = row[0]  # First column contains the partition string
                part_dict = {}
                for part in part_str.split('/'):
                    if '=' in part:
                        k, v = part.split('=', 1)
                        part_dict[k] = v
                if part_dict:
                    partitions.append(part_dict)
                    
            return partitions if partitions else None
            
        except Exception as e:
            logger.warning(f"Could not get partitions for {database}.{table}: {str(e)}")
            return None
    
    def fetch_data(self, environment: str, database: str, table: str, limit: Optional[int] = None, 
                  columns: Optional[List[str]] = None, 
                  partition: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Fetch data from a Hive table in specific environment with optional filtering and limiting
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            database (str): Database name
            table (str): Table name
            limit (int, optional): Maximum number of rows to return
            columns (List[str], optional): List of columns to select
            partition (Dict[str, str], optional): Partition to filter on
            
        Returns:
            pd.DataFrame: DataFrame containing the query results
        """
        try:
            # Build the SELECT clause
            if columns:
                select_clause = ", ".join(columns)
            else:
                select_clause = "*"
                
            # Build the WHERE clause for partition if specified
            where_clause = ""
            if partition:
                conditions = [f"{k}='{v}'" for k, v in partition.items()]
                where_clause = " WHERE " + " AND ".join(conditions)
                
            # Build the LIMIT clause if specified
            limit_clause = f" LIMIT {limit}" if limit else ""
            
            # Build and execute the query
            query = f"SELECT {select_clause} FROM {database}.{table}{where_clause}{limit_clause}"
            
            # Execute query on specific environment
            results = self.connection_manager.execute_query(environment, query)
            
            # Get column names from the first query execution
            # We need to execute DESCRIBE to get column info since execute_query returns raw results
            describe_query = f"DESCRIBE {database}.{table}"
            describe_results = self.connection_manager.execute_query(environment, describe_query)
            column_names = [row[0] for row in describe_results] if describe_results else []
            
            # If we have specific columns requested, use those instead
            if columns:
                column_names = columns
            
            # Create DataFrame
            df = pd.DataFrame(results, columns=column_names[:len(results[0])] if results else [])
            
            # Remove table prefixes from column names
            df.columns = [col.split('.')[-1] if '.' in col else col for col in df.columns]
            
            part_str = f"partition {partition}" if partition else "full table"
            logger.info(f"Fetched {len(df)} rows from {database}.{table} {part_str}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch data from {database}.{table}: {str(e)}")
            return None
    
    def get_table_schema(self, environment: str, database: str, table: str) -> Dict[str, str]:
        """
        Get the actual schema of a table from Hive in specific environment
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            database (str): Database name
            table (str): Table name
            
        Returns:
            Dict[str, str]: Schema dictionary (column_name -> data_type)
        """
        try:
            # Get detailed column information including data types
            schema_query = f"DESCRIBE FORMATTED {database}.{table}"
            
            # Execute query on specific environment
            results = self.connection_manager.execute_query(environment, schema_query)
            
            # Create DataFrame for easier processing - DESCRIBE FORMATTED returns 3 columns
            column_names = ['col_name', 'data_type', 'comment']
            schema_df = pd.DataFrame(results, columns=column_names)
            
            # Parse the schema information
            actual_schema = {}
            
            for _, row in schema_df.iterrows():
                col_name = str(row['col_name']).strip() if pd.notna(row.get('col_name')) else ""
                data_type = str(row['data_type']).strip() if pd.notna(row.get('data_type')) else ""
                
                # Skip empty rows and section headers
                if not col_name or col_name.startswith('#') or col_name == '':
                    continue
                
                # Stop at detailed table information section
                if col_name.lower().startswith('detailed table information'):
                    break
                    
                # Skip storage and other metadata sections
                if any(keyword in col_name.lower() for keyword in ['storage', 'location', 'serde', 'input', 'output', 'parameters']):
                    break
                
                # Add valid column schema
                if data_type and not data_type.startswith('#'):
                    actual_schema[col_name.lower()] = data_type.lower()
            
            logger.info(f"Retrieved schema for {database}.{table}: {len(actual_schema)} columns")
            return actual_schema
            
        except Exception as e:
            logger.error(f"Failed to get schema for {database}.{table}: {str(e)}")
            return {}
    
    def get_table_data(self, database: str, table: str, partition: Optional[Dict[str, str]] = None) -> Optional[pd.DataFrame]:
        """
        Fetch data from a specific table, optionally filtered by partition
        
        This is a compatibility method that wraps fetch_data()
        
        Args:
            database (str): Database name
            table (str): Table name
            partition (Dict[str, str], optional): Partition filter
            
        Returns:
            pd.DataFrame: Table data or None if fetch failed
        """
        return self.fetch_data(database, table, partition=partition)

    def get_table_info(self, database: str, table: str) -> Dict[str, Any]:
        """
        Get comprehensive table information including partitions and schema
        
        Args:
            database (str): Database name
            table (str): Table name
            
        Returns:
            Dict[str, Any]: Table information
        """
        info = {
            'database': database,
            'table': table,
            'is_partitioned': False,
            'partitions': None,
            'schema': {},
            'partition_count': 0
        }
        
        # Get partitions
        partitions = self.get_table_partitions(database, table)
        if partitions:
            info['is_partitioned'] = True
            info['partitions'] = partitions
            info['partition_count'] = len(partitions)
        
        # Get schema
        info['schema'] = self.get_table_schema(database, table)
        
        return info

    def get_all_tables_info(self, database_table_combinations: List[tuple]) -> Dict[str, Dict[str, Any]]:
        """
        Get information for all database-table combinations
        
        Args:
            database_table_combinations (List[tuple]): List of (database, table) tuples
            
        Returns:
            Dict[str, Dict[str, Any]]: Nested dictionary with database->table->info structure
        """
        all_info = {}
        
        for database, table in database_table_combinations:
            if database not in all_info:
                all_info[database] = {}
            
            logger.info(f"Getting info for {database}.{table}")
            table_info = self.get_table_info(database, table)
            all_info[database][table] = table_info
            
        return all_info

    def validate_database_table_existence(self, database_table_combinations: List[tuple]) -> Dict[str, bool]:
        """
        Validate that all specified database-table combinations exist
        
        Args:
            database_table_combinations (List[tuple]): List of (database, table) tuples
            
        Returns:
            Dict[str, bool]: Dictionary mapping "database.table" to existence status
        """
        existence_status = {}
        
        for database, table in database_table_combinations:
            key = f"{database}.{table}"
            try:
                # Try to get basic table info to check existence
                query = f"DESCRIBE {database}.{table}"
                conn = self.connection_manager.get_connection(use_sqlalchemy=False)
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                existence_status[key] = len(results) > 0
                logger.info(f"✅ {key} exists")
            except Exception as e:
                existence_status[key] = False
                logger.warning(f"❌ {key} does not exist or is not accessible: {str(e)}")
                
        return existence_status
