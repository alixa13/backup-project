#!/usr/bin/env python3
"""
Script to retrieve and display connection logs from PostgreSQL database.
Shows all 41 features for each connection log.
"""

import psycopg2
import numpy as np
from datetime import datetime
import sys

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'lstm_db',
    'user': 'lstm_user',
    'password': 'lstm_password'
}

# Number of logs to retrieve
NUM_LOGS = 5

# Expected features for conn logs (from improved feature engineering)
EXPECTED_FEATURES = 41

def connect_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Connected to database: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

def get_conn_logs(conn, limit=5):
    """Retrieve connection logs from database."""
    cursor = conn.cursor()
    
    try:
        query = """
            SELECT id, log_type, features, timestamp 
            FROM collected_data_rows 
            WHERE log_type = 'conn' 
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        
        print(f"✓ Retrieved {len(results)} connection logs\n")
        return results
        
    except Exception as e:
        print(f"✗ Error querying database: {e}")
        return []
    finally:
        cursor.close()

def print_features(row_id, log_type, features_bytes, timestamp):
    """Decode and print all features for a connection log."""
    
    # Decode binary features to numpy array
    try:
        features = np.frombuffer(bytes(features_bytes), dtype=np.float64)
        
        if len(features) != EXPECTED_FEATURES:
            print(f"⚠ Warning: Expected {EXPECTED_FEATURES} features, got {len(features)}")
        
        print("=" * 80)
        print(f"LOG ID: {row_id}")
        print(f"Type: {log_type}")
        print(f"Timestamp: {timestamp}")
        print(f"Total Features: {len(features)}")
        print("-" * 80)
        
        # Print all features with indices
        print("\nALL FEATURES (41 dimensions):")
        print("-" * 80)
        
        # Print in groups of 5 for readability
        for i in range(0, len(features), 5):
            feature_group = features[i:i+5]
            for j, value in enumerate(feature_group):
                idx = i + j
                print(f"  Feature[{idx:2d}]: {value:15.6f}", end="")
                if (j + 1) % 5 == 0 or idx == len(features) - 1:
                    print()  # New line after every 5 features
            
        print("-" * 80)
        
        # Print statistics
        print(f"\nSTATISTICS:")
        print(f"  Min value:  {np.min(features):15.6f}")
        print(f"  Max value:  {np.max(features):15.6f}")
        print(f"  Mean value: {np.mean(features):15.6f}")
        print(f"  Std dev:    {np.std(features):15.6f}")
        print("=" * 80)
        print()
        
    except Exception as e:
        print(f"✗ Error decoding features: {e}")

def get_total_count(conn):
    """Get total count of connection logs in database."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM collected_data_rows 
            WHERE log_type = 'conn'
        """)
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"✗ Error getting count: {e}")
        return 0
    finally:
        cursor.close()

def main():
    """Main function."""
    print("\n" + "=" * 80)
    print(" CONNECTION LOGS VIEWER - PostgreSQL Database")
    print("=" * 80)
    print()
    
    # Connect to database
    conn = connect_db()
    
    # Get total count
    total_count = get_total_count(conn)
    print(f"Total connection logs in database: {total_count}")
    print()
    
    # Retrieve logs
    logs = get_conn_logs(conn, limit=NUM_LOGS)
    
    if not logs:
        print("No connection logs found in database.")
        conn.close()
        return
    
    # Process and display each log
    for row in logs:
        row_id, log_type, features_bytes, timestamp = row
        print_features(row_id, log_type, features_bytes, timestamp)
    
    # Close connection
    conn.close()
    print("✓ Database connection closed")
    print()

if __name__ == "__main__":
    # Check if user wants to specify number of logs
    if len(sys.argv) > 1:
        try:
            NUM_LOGS = int(sys.argv[1])
            print(f"Retrieving {NUM_LOGS} logs...")
        except ValueError:
            print(f"Invalid number: {sys.argv[1]}, using default: {NUM_LOGS}")
    
    main()
