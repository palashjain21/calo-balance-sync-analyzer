import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import logging

class DataAnalyzer:
    """
    Analyze transaction data to generate insights and trends.
    Provides statistical analysis and visualization data.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def generate_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive summary statistics."""
        if df.empty:
            return {}
        
        # Ensure string columns are properly typed to avoid categorical issues
        # Convert any categorical columns to string
        for col in df.columns:
            try:
                # Use modern pandas check for categorical data
                if isinstance(df[col].dtype, pd.CategoricalDtype):
                    df[col] = df[col].astype(str)
                elif df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
            except:
                # Fallback for older pandas versions
                if hasattr(df[col], 'cat') or str(df[col].dtype) == 'category':
                    df[col] = df[col].astype(str)
                elif df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
        
        # Specific columns that should be strings
        string_columns = ['transaction_type', 'operation', 'subscriber_id', 'source_file', 'folder_path', 'status']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        summary = {
            'data_overview': {
                'total_transactions': len(df),
                'date_range': {
                    'start': df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S') if 'timestamp' in df.columns else 'N/A',
                    'end': df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S') if 'timestamp' in df.columns else 'N/A'
                },
                'unique_subscribers': df['subscriber_id'].nunique(),
                'total_transaction_volume': df['amount'].sum(),
                # Add multi-file support information
                'source_files': df['source_file'].nunique() if 'source_file' in df.columns else 1,
                'files_processed': df['source_file'].unique().tolist() if 'source_file' in df.columns else [],
                'folders_processed': df['folder_path'].unique().tolist() if 'folder_path' in df.columns else []
            },
            'transaction_analysis': {
                'avg_transaction_amount': df['amount'].mean(),
                'median_transaction_amount': df['amount'].median(),
                'largest_transaction': df['amount'].max(),
                'smallest_transaction': df['amount'].min(),
                'transaction_types': self._safe_value_counts(df, 'transaction_type')
            },
            'balance_analysis': {
                'subscribers_with_overdrafts': df[df['is_overdraft']]['subscriber_id'].nunique() if 'is_overdraft' in df.columns else 0,
                'total_overdraft_instances': df['is_overdraft'].sum() if 'is_overdraft' in df.columns else 0,
                'avg_running_balance': df['running_balance'].mean() if 'running_balance' in df.columns else 0
            },
            'operational_metrics': {
                'success_rate': (df['status'] == 'success').mean() * 100 if 'status' in df.columns else 0,
                'avg_processing_time': df['duration_ms'].mean() if 'duration_ms' in df.columns else 0,
                'operations_by_type': self._safe_value_counts(df, 'operation')
            }
        }
        
        return summary
    
    def _safe_value_counts(self, df: pd.DataFrame, column: str) -> Dict:
        """Safely perform value_counts operation with proper error handling."""
        try:
            if column not in df.columns:
                return {}
            
            # Ensure the column is string type
            series = df[column].astype(str)
            
            # Handle any remaining categorical issues
            if hasattr(series, 'cat'):
                series = series.astype(str)
            
            return series.value_counts().to_dict()
            
        except Exception as e:
            self.logger.warning(f"Error in value_counts for column {column}: {str(e)}")
            return {}
    
    def analyze_subscriber_behavior(self, df: pd.DataFrame) -> Dict:
        """Analyze individual subscriber behavior patterns."""
        subscriber_analysis = {}
        
        # Ensure string type to avoid categorical issues
        if 'subscriber_id' in df.columns:
            df['subscriber_id'] = df['subscriber_id'].astype(str)
        if 'transaction_type' in df.columns:
            df['transaction_type'] = df['transaction_type'].astype(str)
        
        for subscriber_id in df['subscriber_id'].unique():
            if subscriber_id == 'unknown':
                continue
            
            subscriber_df = df[df['subscriber_id'] == subscriber_id].copy()
            
            # Calculate behavioral metrics
            preferred_type = 'unknown'
            if 'transaction_type' in subscriber_df.columns and len(subscriber_df) > 0:
                type_mode = subscriber_df['transaction_type'].mode()
                if len(type_mode) > 0:
                    preferred_type = type_mode.iloc[0]
            
            analysis = {
                'total_transactions': len(subscriber_df),
                'total_volume': subscriber_df['amount'].sum(),
                'avg_transaction_size': subscriber_df['amount'].mean(),
                'transaction_frequency': self._calculate_frequency(subscriber_df),
                'preferred_transaction_type': preferred_type,
                'risk_score': self._calculate_risk_score(subscriber_df),
                'balance_stability': self._calculate_balance_stability(subscriber_df)
            }
            
            subscriber_analysis[subscriber_id] = analysis
        
        return subscriber_analysis
    
    def _calculate_frequency(self, subscriber_df: pd.DataFrame) -> str:
        """Calculate transaction frequency for a subscriber."""
        if len(subscriber_df) < 2:
            return 'insufficient_data'
        
        time_span = (subscriber_df['timestamp'].max() - subscriber_df['timestamp'].min()).days
        if time_span == 0:
            return 'same_day'
        
        transactions_per_day = len(subscriber_df) / time_span
        
        if transactions_per_day >= 1:
            return 'daily'
        elif transactions_per_day >= 0.2:
            return 'weekly'
        else:
            return 'monthly'
    
    def _calculate_risk_score(self, subscriber_df: pd.DataFrame) -> float:
        """Calculate risk score based on transaction patterns."""
        risk_factors = 0
        
        # Factor 1: Overdraft frequency
        if 'is_overdraft' in subscriber_df.columns:
            overdraft_rate = subscriber_df['is_overdraft'].mean()
            risk_factors += overdraft_rate * 40
        
        # Factor 2: Transaction size volatility
        amount_std = subscriber_df['amount'].std()
        amount_mean = subscriber_df['amount'].mean()
        if amount_mean > 0:
            volatility = amount_std / amount_mean
            risk_factors += min(volatility * 30, 30)
        
        # Factor 3: Failed transaction rate
        if 'status' in subscriber_df.columns:
            failure_rate = (subscriber_df['status'] == 'failed').mean()
            risk_factors += failure_rate * 30
        
        return min(risk_factors, 100)  # Cap at 100
    
    def _calculate_balance_stability(self, subscriber_df: pd.DataFrame) -> str:
        """Calculate balance stability rating."""
        if 'running_balance' not in subscriber_df.columns or len(subscriber_df) < 3:
            return 'unknown'
        
        balance_std = subscriber_df['running_balance'].std()
        balance_mean = abs(subscriber_df['running_balance'].mean())
        
        if balance_mean == 0:
            return 'unknown'
        
        coefficient_of_variation = balance_std / balance_mean
        
        if coefficient_of_variation < 0.2:
            return 'stable'
        elif coefficient_of_variation < 0.5:
            return 'moderate'
        else:
            return 'volatile'
    
    def detect_trends(self, df: pd.DataFrame) -> Dict:
        """Detect trends in transaction data."""
        trends = {}
        
        if df.empty or 'timestamp' not in df.columns:
            return trends
        
        # Daily transaction trends
        df['date'] = df['timestamp'].dt.date
        daily_stats = df.groupby('date').agg({
            'amount': ['sum', 'count', 'mean'],
            'is_overdraft': 'sum' if 'is_overdraft' in df.columns else lambda x: 0
        }).reset_index()
        
        daily_stats.columns = ['date', 'total_volume', 'transaction_count', 'avg_amount', 'overdraft_count']
        
        trends['daily_trends'] = {
            'volume_trend': self._calculate_trend(daily_stats['total_volume']),
            'count_trend': self._calculate_trend(daily_stats['transaction_count']),
            'overdraft_trend': self._calculate_trend(daily_stats['overdraft_count'])
        }
        
        # Weekly patterns
        df['weekday'] = df['timestamp'].dt.day_name()
        weekly_pattern = df.groupby('weekday')['amount'].agg(['sum', 'count']).reset_index()
        trends['weekly_patterns'] = weekly_pattern.to_dict('records')
        
        # Hourly patterns
        df['hour'] = df['timestamp'].dt.hour
        hourly_pattern = df.groupby('hour')['amount'].agg(['sum', 'count']).reset_index()
        trends['hourly_patterns'] = hourly_pattern.to_dict('records')
        
        return trends
    
    def _calculate_trend(self, series: pd.Series) -> str:
        """Calculate trend direction from a time series."""
        if len(series) < 2:
            return 'insufficient_data'
        
        # Simple linear regression slope
        x = np.arange(len(series))
        slope = np.polyfit(x, series, 1)[0]
        
        if slope > 0.1:
            return 'increasing'
        elif slope < -0.1:
            return 'decreasing'
        else:
            return 'stable'
    
    def generate_visualizations(self, df: pd.DataFrame) -> Dict:
        """Generate visualization data for charts."""
        if df.empty:
            return {}
        
        visualizations = {}
        
        # Transaction volume over time
        if 'timestamp' in df.columns:
            df['date'] = df['timestamp'].dt.date
            daily_volume = df.groupby('date')['amount'].sum().reset_index()
            
            visualizations['volume_chart'] = {
                'x': [str(date) for date in daily_volume['date'].tolist()],
                'y': [float(amount) for amount in daily_volume['amount'].tolist()],
                'type': 'line',
                'title': 'Daily Transaction Volume'
            }
        
        # Transaction type distribution
        if 'transaction_type' in df.columns:
            # Ensure string type to avoid categorical issues
            df['transaction_type'] = df['transaction_type'].astype(str)
            # Convert to string to avoid categorical issues
            df['transaction_type'] = df['transaction_type'].astype(str)
            type_counts = df['transaction_type'].value_counts()
            
            visualizations['type_distribution'] = {
                'labels': [str(label) for label in type_counts.index.tolist()],
                'values': [float(val) for val in type_counts.values.tolist()],
                'type': 'pie',
                'title': 'Transaction Type Distribution'
            }
        
        # Balance trends for top subscribers
        if 'running_balance' in df.columns and 'subscriber_id' in df.columns:
            # Ensure string type to avoid categorical issues
            df['subscriber_id'] = df['subscriber_id'].astype(str)
            # Convert to string to avoid categorical issues
            df['subscriber_id'] = df['subscriber_id'].astype(str)
            top_subscribers = df['subscriber_id'].value_counts().head(5).index
            balance_data = []
            
            for subscriber in top_subscribers:
                if subscriber != 'unknown':
                    sub_data = df[df['subscriber_id'] == subscriber].sort_values('timestamp')
                    balance_data.append({
                        'subscriber': str(subscriber),
                        'timestamps': [str(ts) for ts in sub_data['timestamp'].dt.strftime('%Y-%m-%d %H:%M').tolist()],
                        'balances': [float(balance) for balance in sub_data['running_balance'].tolist()]
                    })
            
            visualizations['balance_trends'] = balance_data
        
        # Overdraft analysis
        if 'is_overdraft' in df.columns:
            overdraft_by_date = df.groupby(df['timestamp'].dt.date)['is_overdraft'].sum().reset_index()
            
            visualizations['overdraft_timeline'] = {
                'x': [str(date) for date in overdraft_by_date['timestamp'].tolist()],
                'y': [int(count) for count in overdraft_by_date['is_overdraft'].tolist()],
                'type': 'bar',
                'title': 'Daily Overdraft Incidents'
            }
        
        return visualizations
    
    def generate_recommendations(self, summary_stats: Dict, subscriber_analysis: Dict) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []
        
        # Check overdraft rates
        if summary_stats.get('balance_analysis', {}).get('total_overdraft_instances', 0) > 0:
            recommendations.append("High overdraft activity detected. Consider implementing real-time balance monitoring and alerts.")
        
        # Check transaction failure rates
        success_rate = summary_stats.get('operational_metrics', {}).get('success_rate', 100)
        if success_rate < 95:
            recommendations.append(f"Transaction success rate is {success_rate:.1f}%. Investigate system reliability issues.")
        
        # Check high-risk subscribers
        high_risk_subscribers = [
            sub_id for sub_id, analysis in subscriber_analysis.items() 
            if analysis.get('risk_score', 0) > 70
        ]
        
        if high_risk_subscribers:
            recommendations.append(f"Found {len(high_risk_subscribers)} high-risk subscribers. Consider enhanced monitoring.")
        
        # Check processing times
        avg_duration = summary_stats.get('operational_metrics', {}).get('avg_processing_time', 0)
        if avg_duration > 1000:  # More than 1 second
            recommendations.append("High processing times detected. Consider system performance optimization.")
        
        # General recommendations
        recommendations.append("Implement automated daily reconciliation reports to catch issues early.")
        recommendations.append("Set up real-time alerts for transactions exceeding normal patterns.")
        recommendations.append("Consider implementing predictive models for overdraft prevention.")
        
        return recommendations
