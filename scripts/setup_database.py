"""
Database setup script for Data Quality Framework
Creates the PostgreSQL database and tables
"""

import sys
import os
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import POSTGRES_CONFIG
from storage import PostgreSQLStorage
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database():
    """Create the data_quality_db database if it doesn't exist"""
    try:
        # Connect to PostgreSQL server (not to specific database)
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['connection']['host'],
            port=POSTGRES_CONFIG['connection']['port'],
            user=POSTGRES_CONFIG['connection']['user'],
            password=POSTGRES_CONFIG['connection']['password'],
            database='postgres'  # Connect to default postgres database
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", 
                      (POSTGRES_CONFIG['connection']['database'],))
        
        if not cursor.fetchone():
            # Create database
            cursor.execute(f"CREATE DATABASE {POSTGRES_CONFIG['connection']['database']}")
            logger.info(f"Created database: {POSTGRES_CONFIG['connection']['database']}")
        else:
            logger.info(f"Database {POSTGRES_CONFIG['connection']['database']} already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {str(e)}")
        return False

def setup_tables():
    """Setup all required tables"""
    try:
        storage = PostgreSQLStorage(POSTGRES_CONFIG['connection'])
        
        if not storage.connect():
            logger.error("Failed to connect to PostgreSQL")
            return False
        
        if not storage.create_tables():
            logger.error("Failed to create tables")
            return False
        
        storage.disconnect()
        logger.info("Successfully created all tables")
        return True
        
    except Exception as e:
        logger.error(f"Failed to setup tables: {str(e)}")
        return False

def main():
    """Main setup function"""
    print("Setting up Data Quality Framework Database...")
    print("=" * 50)
    
    # Step 1: Create database
    print("Step 1: Creating database...")
    if not create_database():
        print("❌ Failed to create database")
        sys.exit(1)
    print("✅ Database ready")
    
    # Step 2: Create tables
    print("\nStep 2: Creating tables...")
    if not setup_tables():
        print("❌ Failed to create tables")
        sys.exit(1)
    print("✅ Tables created successfully")
    
    print("\n" + "=" * 50)
    print("🎉 Database setup completed successfully!")
    print("\nTables created:")
    print("- data_quality.summary_metrics_table") 
    print("- data_quality.summary_metrics_database")
    print("- data_quality.dimension_scores")
    print("- data_quality.freshness")
    print("\nYour Data Quality Framework is now ready to store results in PostgreSQL!")

if __name__ == "__main__":
    main()
