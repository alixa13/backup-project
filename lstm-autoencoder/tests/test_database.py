"""
Test script to verify database operations and LSTM autoencoder functionality.
"""
import os
import sys
import unittest
import numpy as np
from datetime import datetime
import sqlite3
import json

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.database import Database
from app.model import LSTMAutoencoder

class TestDatabase(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        # Use a test database file
        self.test_db_path = '/tmp/test_lstm_autoencoder.db'
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        self.db = Database(db_path=self.test_db_path)
        
        # Initialize test data
        self.test_data = np.array([
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
            [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
        ])
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
    
    def test_database_initialization(self):
        """Test if database and tables are created correctly."""
        # Check if database file exists
        self.assertTrue(os.path.exists(self.test_db_path))
        
        # Check if tables exist
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            
            # Check learning_status table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='learning_status'")
            self.assertIsNotNone(cursor.fetchone())
            
            # Check collected_data table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='collected_data'")
            self.assertIsNotNone(cursor.fetchone())
            
            # Check total_rows table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='total_rows'")
            self.assertIsNotNone(cursor.fetchone())
    
    def test_learning_status(self):
        """Test learning status operations."""
        # Test initial status (should be False)
        self.assertFalse(self.db.get_learning_status())
        
        # Test enabling learning
        self.assertTrue(self.db.set_learning_status(True))
        self.assertTrue(self.db.get_learning_status())
        
        # Test disabling learning
        self.assertTrue(self.db.set_learning_status(False))
        self.assertFalse(self.db.get_learning_status())
    
    def test_data_collection(self):
        """Test data collection operations."""
        # Test saving data
        self.assertTrue(self.db.save_collected_data(self.test_data))
        
        # Test loading data
        loaded_data = self.db.load_collected_data()
        self.assertIsNotNone(loaded_data)
        self.assertEqual(loaded_data.shape, self.test_data.shape)
        np.testing.assert_array_almost_equal(loaded_data, self.test_data)
        
        # Test data size
        self.assertEqual(self.db.get_data_size(), len(self.test_data))
        
        # Test clearing data
        self.assertTrue(self.db.clear_collected_data())
        self.assertEqual(self.db.get_data_size(), 0)
        self.assertIsNone(self.db.load_collected_data())
    
    def test_total_rows(self):
        """Test total rows operations."""
        # Test initial total rows (should be 0)
        self.assertEqual(self.db.get_total_rows(), 0)
        
        # Test updating total rows
        self.assertEqual(self.db.update_total_rows(10), 10)
        self.assertEqual(self.db.get_total_rows(), 10)
        
        # Test updating total rows again
        self.assertEqual(self.db.update_total_rows(5), 15)
        self.assertEqual(self.db.get_total_rows(), 15)
    
    def test_concurrent_access(self):
        """Test concurrent access to database."""
        import threading
        
        def worker():
            for _ in range(10):
                self.db.save_collected_data(self.test_data)
                self.db.get_learning_status()
                self.db.get_total_rows()
        
        # Create multiple threads
        threads = [threading.Thread(target=worker) for _ in range(5)]
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify data integrity
        self.assertEqual(self.db.get_data_size(), 50)  # 5 threads * 10 iterations * 3 rows
    
    def test_data_types(self):
        """Test handling of different data types."""
        # Test with mixed data types
        mixed_data = np.array([
            [1, 2.5, "3", 4, 5, 6, 7, 8, 9, 10],
            [2, 3.5, "4", 5, 6, 7, 8, 9, 10, 11],
            [3, 4.5, "5", 6, 7, 8, 9, 10, 11, 12]
        ])
        
        # Save mixed data
        self.assertTrue(self.db.save_collected_data(mixed_data))
        
        # Load and verify data
        loaded_data = self.db.load_collected_data()
        self.assertIsNotNone(loaded_data)
        self.assertEqual(loaded_data.shape, mixed_data.shape)
        
        # Verify numeric conversion
        for i in range(len(mixed_data)):
            for j in range(len(mixed_data[i])):
                if isinstance(mixed_data[i][j], (int, float)):
                    self.assertEqual(loaded_data[i][j], float(mixed_data[i][j]))
                elif isinstance(mixed_data[i][j], str):
                    try:
                        self.assertEqual(loaded_data[i][j], float(mixed_data[i][j]))
                    except ValueError:
                        self.assertEqual(loaded_data[i][j], 0.0)

def run_tests():
    """Run all tests and print results."""
    print("Starting database tests...")
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
    print("\nDatabase tests completed!")

if __name__ == '__main__':
    run_tests() 