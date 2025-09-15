"""
Hive Connection Manager for Data Quality Framework - Multi-Environment Support
"""

import logging
from pyhive import hive
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class HiveConnectionManager:
    """
    Manages connections to multiple Hive environments (PROD and DEV)
    """
    
    def __init__(self, environments_config):
        """
        Initialize Hive connection manager for multiple environments
        
        Args:
            environments_config (dict): Dictionary containing PROD and DEV Hive configurations
        """
        self.environments_config = environments_config
        self.connections = {}  # Store connections for each environment
        self.cursors = {}      # Store cursors for each environment
        self.logger = logging.getLogger(__name__)
        
    def connect(self, environment):
        """
        Establish connection to specific Hive environment
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if environment not in self.environments_config:
                raise ValueError(f"Environment '{environment}' not found in configuration")
                
            config = self.environments_config[environment]
            
            self.connections[environment] = hive.Connection(
                host=config['host'],
                port=config['port'],
                username=config['username'],
                database=config.get('database', 'default')
            )
            self.cursors[environment] = self.connections[environment].cursor()
            self.logger.info(f"Successfully connected to {environment} Hive at {config['host']}:{config['port']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {environment} Hive: {str(e)}")
            return False
    
    def connect_all(self):
        """
        Establish connections to all configured environments
        
        Returns:
            dict: Dictionary with environment names as keys and connection status as values
        """
        results = {}
        for environment in self.environments_config.keys():
            results[environment] = self.connect(environment)
        return results
    
    def disconnect(self, environment):
        """
        Close specific Hive environment connection
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
        """
        try:
            if environment in self.cursors and self.cursors[environment]:
                self.cursors[environment].close()
                del self.cursors[environment]
                
            if environment in self.connections and self.connections[environment]:
                self.connections[environment].close()
                del self.connections[environment]
                
            self.logger.info(f"{environment} Hive connection closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing {environment} Hive connection: {str(e)}")
    
    def disconnect_all(self):
        """
        Close all Hive environment connections
        """
        for environment in list(self.connections.keys()):
            self.disconnect(environment)
    
    def execute_query(self, environment, query):
        """
        Execute a query on specific Hive environment
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            query (str): SQL query to execute
            
        Returns:
            list: Query results
        """
        try:
            if environment not in self.cursors or not self.cursors[environment]:
                raise Exception(f"No active {environment} Hive connection")
                
            self.cursors[environment].execute(query)
            results = self.cursors[environment].fetchall()
            return results
            
        except Exception as e:
            self.logger.error(f"Error executing query on {environment}: {str(e)}")
            raise
    
    def test_connection(self, environment):
        """
        Test specific Hive environment connection
        
        Args:
            environment (str): Environment name ('PROD' or 'DEV')
            
        Returns:
            bool: True if connection test successful
        """
        try:
            if self.connect(environment):
                # Test with a simple query
                test_query = "SHOW DATABASES"
                results = self.execute_query(environment, test_query)
                self.disconnect(environment)
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"{environment} connection test failed: {str(e)}")
            return False
    
    def test_all_connections(self):
        """
        Test all configured Hive environment connections
        
        Returns:
            dict: Dictionary with environment names as keys and test results as values
        """
        results = {}
        for environment in self.environments_config.keys():
            results[environment] = self.test_connection(environment)
        return results
