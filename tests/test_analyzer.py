import unittest
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from log_parser import LogParser
from balance_tracker import BalanceTracker
from analyzer import DataAnalyzer

class TestLogParser(unittest.TestCase):
    """Test cases for LogParser class."""
    
    def setUp(self):
        self.parser = LogParser()
    
    def test_parse_single_entry(self):
        """Test parsing a single log entry."""
        log_entry = """
        2023-12-12T13:47:53.756Z START RequestId: a0f720e2-91f7-5b84-96f0-7d10f7536749 Version: $LATEST
        2023-12-12T13:47:54.700Z a0f720e2-91f7-5b84-96f0-7d10f7536749 INFO Processing message dee652d2-959f-4c45-ac6f-c088f5274437
        """
        
        parsed = self.parser._parse_single_entry(log_entry)
        
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['request_id'], 'a0f720e2-91f7-5b84-96f0-7d10f7536749')
        self.assertEqual(parsed['message_id'], 'dee652d2-959f-4c45-ac6f-c088f5274437')

class TestBalanceTracker(unittest.TestCase):
    """Test cases for BalanceTracker class."""
    
    def setUp(self):
        self.tracker = BalanceTracker()
    
    def test_balance_calculation(self):
        """Test balance calculation for sample transactions."""
        sample_data = pd.DataFrame([
            {
                'timestamp': datetime.now(),
                'subscriber_id': 'test_sub',
                'transaction_type': 'credit',
                'amount': 100.0,
                'status': 'success'
            },
            {
                'timestamp': datetime.now() + timedelta(minutes=1),
                'subscriber_id': 'test_sub',
                'transaction_type': 'debit',
                'amount': 150.0,
                'status': 'success'
            }
        ])
        
        result_df = self.tracker.process_transactions(sample_data)
        
        self.assertEqual(len(result_df), 2)
        self.assertEqual(result_df.iloc[0]['running_balance'], 100.0)
        self.assertEqual(result_df.iloc[1]['running_balance'], -50.0)
        self.assertTrue(result_df.iloc[1]['is_overdraft'])

class TestDataAnalyzer(unittest.TestCase):
    """Test cases for DataAnalyzer class."""
    
    def setUp(self):
        self.analyzer = DataAnalyzer()
    
    def test_summary_stats_generation(self):
        """Test summary statistics generation."""
        sample_data = pd.DataFrame([
            {
                'timestamp': datetime.now(),
                'subscriber_id': 'sub1',
                'amount': 100.0,
                'transaction_type': 'credit',
                'is_overdraft': False,
                'running_balance': 100.0
            },
            {
                'timestamp': datetime.now(),
                'subscriber_id': 'sub2',
                'amount': 50.0,
                'transaction_type': 'debit',
                'is_overdraft': True,
                'running_balance': -50.0
            }
        ])
        
        stats = self.analyzer.generate_summary_stats(sample_data)
        
        self.assertEqual(stats['data_overview']['total_transactions'], 2)
        self.assertEqual(stats['data_overview']['unique_subscribers'], 2)
        self.assertEqual(stats['balance_analysis']['total_overdraft_instances'], 1)

if __name__ == '__main__':
    unittest.main()
