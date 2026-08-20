#!/usr/bin/env python3
"""
Script to retrieve and display logs from PostgreSQL database.
Supports all log types: conn, dns, http, ssl
Shows all features for each log entry.
"""

import psycopg2
import numpy as np
from datetime import datetime
import sys
import argparse

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'lstm_db',
    'user': 'lstm_user',
    'password': 'lstm_password'
}

# Feature dimensions for each log type (from improved feature engineering)
FEATURE_DIMENSIONS = {
    'conn': 41,
    'dns': 38,
    'http': 52,
    'ssl': 33
}

# Feature names for connection logs (41 features)
CONN_FEATURE_NAMES = [
    "duration", "orig_bytes", "resp_bytes", "missed_bytes",
    "orig_pkts", "orig_ip_bytes", "resp_pkts", "resp_ip_bytes",
    "proto_tcp", "proto_udp", "proto_icmp", "proto_other",
    "service_encoded", "conn_state_encoded",
    "local_orig", "local_resp",
    "hour_sin", "hour_cos", "day_of_week",
    "bytes_ratio", "pkts_ratio", "duration_log",
    "avg_pkt_size_orig", "avg_pkt_size_resp",
    "orig_bytes_per_sec", "resp_bytes_per_sec",
    "total_bytes", "total_packets",
    "is_short_duration", "is_long_duration",
    "is_small_transfer", "is_large_transfer",
    "is_symmetric", "is_asymmetric",
    "payload_entropy_orig", "payload_entropy_resp",
    "inter_packet_time_mean", "inter_packet_time_std",
    "connection_efficiency", "bidirectional_bytes_ratio",
    "packet_size_variance"
]

def connect_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

def get_logs(conn, log_type, limit=5):
    """Retrieve logs from database."""
    cursor = conn.cursor()
    
    try:
        query = """
            SELECT id, log_type, features, timestamp 
            FROM collected_data_rows 
            WHERE log_type = %s 
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        cursor.execute(query, (log_type, limit))
        results = cursor.fetchall()
        return results
        
    except Exception as e:
        print(f"✗ Error querying database: {e}")
        return []
    finally:
        cursor.close()

def print_features(row_id, log_type, features_bytes, timestamp, show_names=False):
    """Decode and print all features for a log entry."""
    
    expected_features = FEATURE_DIMENSIONS.get(log_type, 41)
    
    # Decode binary features to numpy array
    try:
        features = np.frombuffer(bytes(features_bytes), dtype=np.float64)
        
        if len(features) != expected_features:
            print(f"⚠ Warning: Expected {expected_features} features, got {len(features)}")
        
        print("=" * 100)
        print(f"LOG ID: {row_id} | Type: {log_type.upper()} | Timestamp: {timestamp}")
        print(f"Total Features: {len(features)}")
        print("-" * 100)
        
        # Print all features
        print(f"\nALL FEATURES ({len(features)} dimensions):")
        print("-" * 100)
        
        # Use feature names if available and requested
        feature_names = None
        if show_names and log_type == 'conn':
            feature_names = CONN_FEATURE_NAMES
        
        # Print features
        if feature_names and len(feature_names) == len(features):
            # Print with names
            max_name_len = max(len(name) for name in feature_names)
            for i, (name, value) in enumerate(zip(feature_names, features)):
                print(f"  [{i:2d}] {name:{max_name_len}s} = {value:15.6f}")
        else:
            # Print without names, in groups of 5
            for i in range(0, len(features), 5):
                line = "  "
                for j in range(5):
                    idx = i + j
                    if idx < len(features):
                        line += f"[{idx:2d}]: {features[idx]:12.6f}  "
                print(line)
        
        print("-" * 100)
        
        # Print statistics
        print(f"\nSTATISTICS:")
        print(f"  Min:     {np.min(features):15.6f}")
        print(f"  Max:     {np.max(features):15.6f}")
        print(f"  Mean:    {np.mean(features):15.6f}")
        print(f"  Median:  {np.median(features):15.6f}")
        print(f"  Std Dev: {np.std(features):15.6f}")
        print(f"  Non-zero: {np.count_nonzero(features)}/{len(features)}")
        print("=" * 100)
        print()
        
    except Exception as e:
        print(f"✗ Error decoding features: {e}")
        import traceback
        traceback.print_exc()

def get_total_count(conn, log_type):
    """Get total count of logs in database."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM collected_data_rows 
            WHERE log_type = %s
        """, (log_type,))
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"✗ Error getting count: {e}")
        return 0
    finally:
        cursor.close()

def get_all_counts(conn):
    """Get counts for all log types."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT log_type, COUNT(*) as count, 
                   MIN(timestamp) as first_log,
                   MAX(timestamp) as last_log
            FROM collected_data_rows 
            GROUP BY log_type
            ORDER BY log_type
        """)
        return cursor.fetchall()
    except Exception as e:
        print(f"✗ Error getting counts: {e}")
        return []
    finally:
        cursor.close()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='View logs from PostgreSQL database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Show 5 conn logs
  %(prog)s -t dns -n 10             # Show 10 DNS logs
  %(prog)s -t http -n 3 --names     # Show 3 HTTP logs with feature names
  %(prog)s --summary                # Show summary of all log types
        """
    )
    
    parser.add_argument('-t', '--type', 
                       choices=['conn', 'dns', 'http', 'ssl'],
                       default='conn',
                       help='Log type to view (default: conn)')
    
    parser.add_argument('-n', '--number',
                       type=int,
                       default=5,
                       help='Number of logs to retrieve (default: 5)')
    
    parser.add_argument('--names',
                       action='store_true',
                       help='Show feature names (only for conn logs)')
    
    parser.add_argument('--summary',
                       action='store_true',
                       help='Show summary of all log types')
    
    args = parser.parse_args()
    
    print("\n" + "=" * 100)
    print(f" DATABASE LOG VIEWER - PostgreSQL")
    print("=" * 100)
    print()
    
    # Connect to database
    conn = connect_db()
    print(f"✓ Connected to database: {DB_CONFIG['database']}\n")
    
    # Show summary if requested
    if args.summary:
        print("SUMMARY OF ALL LOG TYPES:")
        print("-" * 100)
        counts = get_all_counts(conn)
        if counts:
            print(f"{'Log Type':<10} {'Count':<15} {'First Log':<25} {'Last Log':<25}")
            print("-" * 100)
            for log_type, count, first_log, last_log in counts:
                print(f"{log_type:<10} {count:<15,} {str(first_log):<25} {str(last_log):<25}")
        else:
            print("No logs found in database.")
        print("-" * 100)
        print()
        conn.close()
        return
    
    # Get total count for specified log type
    total_count = get_total_count(conn, args.type)
    print(f"Total {args.type.upper()} logs in database: {total_count:,}")
    print(f"Retrieving latest {args.number} logs...")
    print()
    
    # Retrieve logs
    logs = get_logs(conn, args.type, limit=args.number)
    
    if not logs:
        print(f"No {args.type.upper()} logs found in database.")
        conn.close()
        return
    
    print(f"✓ Retrieved {len(logs)} {args.type.upper()} logs\n")
    
    # Process and display each log
    for row in logs:
        row_id, log_type, features_bytes, timestamp = row
        print_features(row_id, log_type, features_bytes, timestamp, show_names=args.names)
    
    # Close connection
    conn.close()
    print("✓ Database connection closed\n")

if __name__ == "__main__":
    main()
