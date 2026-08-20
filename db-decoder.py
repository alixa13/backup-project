import psycopg2
import numpy as np

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="lstm_db",
    user="lstm_user",
    password="lstm_password"
)

cursor = conn.cursor()

# Query 5 connection logs
cursor.execute("""
    SELECT id, log_type, features, timestamp 
    FROM collected_data_rows 
    WHERE log_type = 'conn' 
    ORDER BY timestamp DESC 
    LIMIT 5
""")

# Process results
for row in cursor.fetchall():
    row_id, log_type, features_bytes, timestamp = row
    
    # Decode binary features to numpy array
    features = np.frombuffer(bytes(features_bytes), dtype=np.float64)
    
    print(f"\nID: {row_id}")
    print(f"Type: {log_type}")
    print(f"Timestamp: {timestamp}")
    print(f"Features (41 dimensions): {features[:5]}... (showing first 5)")
    print(f"Feature shape: {features.shape}")

cursor.close()
conn.close()
