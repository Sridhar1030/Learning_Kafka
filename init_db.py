"""
Database initialization script for Neon Postgres
Creates the orders table with proper schema for idempotent order handling
"""

import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.errors import DuplicateTable

# Load environment variables from .env file
load_dotenv()

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set. Please configure .env file.")


def init_database():
    """Initialize the database schema"""
    try:
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Create orders table with proper schema
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            payload JSONB NOT NULL,
            kafka_metadata JSONB,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        
        -- Create index on status for faster queries
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        
        -- Create index on created_at for time-based queries
        CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
        """
        
        cur.execute(create_table_sql)
        conn.commit()
        
        print("✓ Database schema created successfully!")
        print("✓ Tables and indexes initialized")
        
        # Verify table creation
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name='orders'
        """)
        
        if cur.fetchone():
            print("✓ Orders table verified")
        
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"✗ Error initializing database: {e}")
        return False


if __name__ == "__main__":
    print("Initializing Neon Postgres database...")
    print(f"Connecting to: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'database'}")
    
    if init_database():
        print("\n✓ Database initialization complete!")
    else:
        print("\n✗ Database initialization failed!")
