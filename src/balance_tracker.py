import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

class BalanceTracker:
    """
    Track subscriber balances and detect overdraft situations.
    Maintains running balance calculations and flags anomalies.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.subscriber_balances = {}
        self.overdraft_threshold = 0.0
        self.alerts = []
    
    def process_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process all transactions and calculate running balances."""
        if df.empty:
            return df
        
        # Sort by timestamp to ensure chronological processing
        df = df.sort_values(['subscriber_id', 'timestamp']).reset_index(drop=True)
        
        # Initialize tracking columns
        df['running_balance'] = 0.0
        df['is_overdraft'] = False
        df['balance_change'] = 0.0
        
        # Process each subscriber separately
        for subscriber_id in df['subscriber_id'].unique():
            if subscriber_id == 'unknown':
                continue
                
            subscriber_df = df[df['subscriber_id'] == subscriber_id].copy()
            updated_df = self._process_subscriber_transactions(subscriber_df)
            
            # Update main dataframe
            df.loc[df['subscriber_id'] == subscriber_id, 'running_balance'] = updated_df['running_balance']
            df.loc[df['subscriber_id'] == subscriber_id, 'is_overdraft'] = updated_df['is_overdraft']
            df.loc[df['subscriber_id'] == subscriber_id, 'balance_change'] = updated_df['balance_change']
        
        return df
    
    def _process_subscriber_transactions(self, subscriber_df: pd.DataFrame) -> pd.DataFrame:
        """Process transactions for a single subscriber."""
        subscriber_id = subscriber_df.iloc[0]['subscriber_id']
        current_balance = self.subscriber_balances.get(subscriber_id, 0.0)
        
        for idx, row in subscriber_df.iterrows():
            # Calculate balance change based on transaction type
            balance_change = self._calculate_balance_change(row)
            current_balance += balance_change
            
            # Update dataframe
            subscriber_df.loc[idx, 'balance_change'] = balance_change
            subscriber_df.loc[idx, 'running_balance'] = current_balance
            subscriber_df.loc[idx, 'is_overdraft'] = current_balance < self.overdraft_threshold
            
            # Generate alerts for overdrafts
            if current_balance < self.overdraft_threshold:
                self._generate_overdraft_alert(subscriber_id, current_balance, row)
        
        # Update stored balance
        self.subscriber_balances[subscriber_id] = current_balance
        
        return subscriber_df
    
    def _calculate_balance_change(self, transaction: pd.Series) -> float:
        """Calculate balance change based on transaction type."""
        amount = transaction.get('amount', 0.0)
        transaction_type = transaction.get('transaction_type', '').lower()
        operation = transaction.get('operation', '').lower()
        status = transaction.get('status', '').lower()
        
        # Skip failed transactions
        if status == 'failed' or status == 'error':
            return 0.0
        
        # Credit transactions (increase balance)
        if transaction_type in ['credit', 'payment', 'refund']:
            return amount
        
        # Debit transactions (decrease balance)
        elif transaction_type in ['debit', 'charge', 'withdrawal']:
            return -amount
        
        # Operation-based logic
        elif operation == 'payment':
            return amount
        elif operation == 'charge':
            return -amount
        
        # Default: no change for unknown transactions
        return 0.0
    
    def _generate_overdraft_alert(self, subscriber_id: str, balance: float, transaction: pd.Series):
        """Generate an alert for overdraft situation."""
        alert = {
            'timestamp': transaction.get('timestamp'),
            'subscriber_id': subscriber_id,
            'current_balance': balance,
            'transaction_amount': transaction.get('amount', 0),
            'transaction_type': transaction.get('transaction_type'),
            'severity': 'high' if balance < -100 else 'medium',
            'message': f"Overdraft detected: ${abs(balance):.2f}"
        }
        
        self.alerts.append(alert)
        self.logger.warning(f"Overdraft alert: Subscriber {subscriber_id} balance: ${balance:.2f}")
    
    def get_overdraft_summary(self, df: pd.DataFrame) -> Dict:
        """Generate summary of overdraft situations."""
        overdraft_df = df[df['is_overdraft']].copy()
        
        summary = {
            'total_overdrafts': len(overdraft_df),
            'unique_subscribers_with_overdrafts': overdraft_df['subscriber_id'].nunique(),
            'total_overdraft_amount': abs(overdraft_df[overdraft_df['running_balance'] < 0]['running_balance'].sum()),
            'avg_overdraft_amount': abs(overdraft_df[overdraft_df['running_balance'] < 0]['running_balance'].mean()) if len(overdraft_df) > 0 else 0,
            'overdraft_by_subscriber': {}
        }
        
        # Per-subscriber overdraft analysis
        for subscriber_id in overdraft_df['subscriber_id'].unique():
            subscriber_overdrafts = overdraft_df[overdraft_df['subscriber_id'] == subscriber_id]
            min_balance = subscriber_overdrafts['running_balance'].min()
            
            summary['overdraft_by_subscriber'][subscriber_id] = {
                'overdraft_count': len(subscriber_overdrafts),
                'worst_balance': min_balance,
                'current_balance': df[df['subscriber_id'] == subscriber_id]['running_balance'].iloc[-1] if len(df[df['subscriber_id'] == subscriber_id]) > 0 else 0
            }
        
        return summary
    
    def get_balance_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze balance trends over time."""
        if df.empty:
            return {}
        
        trends = {}
        
        for subscriber_id in df['subscriber_id'].unique():
            if subscriber_id == 'unknown':
                continue
            
            subscriber_df = df[df['subscriber_id'] == subscriber_id].copy()
            
            if len(subscriber_df) < 2:
                continue
            
            # Calculate trend metrics
            balances = subscriber_df['running_balance'].values
            
            trend_analysis = {
                'initial_balance': balances[0],
                'final_balance': balances[-1],
                'max_balance': np.max(balances),
                'min_balance': np.min(balances),
                'balance_volatility': np.std(balances),
                'trend_direction': 'increasing' if balances[-1] > balances[0] else 'decreasing',
                'transaction_count': len(subscriber_df),
                'days_active': (subscriber_df['timestamp'].max() - subscriber_df['timestamp'].min()).days
            }
            
            trends[subscriber_id] = trend_analysis
        
        return trends
    
    def detect_anomalies(self, df: pd.DataFrame) -> List[Dict]:
        """Detect anomalous patterns in transactions."""
        anomalies = []
        
        for subscriber_id in df['subscriber_id'].unique():
            if subscriber_id == 'unknown':
                continue
            
            subscriber_df = df[df['subscriber_id'] == subscriber_id].copy()
            
            if len(subscriber_df) < 5:  # Need minimum transactions for analysis
                continue
            
            # Detect large transactions
            amounts = subscriber_df['amount'].values
            amount_threshold = np.percentile(amounts, 95)  # Top 5% of transactions
            
            large_transactions = subscriber_df[subscriber_df['amount'] > amount_threshold]
            
            for _, transaction in large_transactions.iterrows():
                anomalies.append({
                    'type': 'large_transaction',
                    'subscriber_id': subscriber_id,
                    'timestamp': transaction['timestamp'],
                    'amount': transaction['amount'],
                    'description': f"Unusually large transaction: ${transaction['amount']:.2f}"
                })
            
            # Detect rapid consecutive transactions
            subscriber_df['time_diff'] = subscriber_df['timestamp'].diff()
            rapid_transactions = subscriber_df[subscriber_df['time_diff'] < timedelta(minutes=5)]
            
            if len(rapid_transactions) > 0:
                anomalies.append({
                    'type': 'rapid_transactions',
                    'subscriber_id': subscriber_id,
                    'timestamp': rapid_transactions.iloc[0]['timestamp'],
                    'count': len(rapid_transactions),
                    'description': f"{len(rapid_transactions)} transactions within 5 minutes"
                })
            
            # Detect balance swings
            balance_changes = np.diff(subscriber_df['running_balance'].values)
            large_swings = np.where(np.abs(balance_changes) > np.std(balance_changes) * 3)[0]
            
            for swing_idx in large_swings:
                anomalies.append({
                    'type': 'balance_swing',
                    'subscriber_id': subscriber_id,
                    'timestamp': subscriber_df.iloc[swing_idx + 1]['timestamp'],
                    'change': balance_changes[swing_idx],
                    'description': f"Large balance swing: ${balance_changes[swing_idx]:.2f}"
                })
        
        return anomalies
    
    def get_current_balances(self) -> Dict[str, float]:
        """Get current balances for all subscribers."""
        return self.subscriber_balances.copy()
    
    def get_alerts(self) -> List[Dict]:
        """Get all generated alerts."""
        return self.alerts.copy()
    
    def reset(self):
        """Reset all tracking data."""
        self.subscriber_balances = {}
        self.alerts = []
