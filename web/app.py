import os
import sys
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, flash
import pandas as pd
import json

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from log_parser import LogParser
from balance_tracker import BalanceTracker
from analyzer import DataAnalyzer
from report_generator import ReportGenerator

app = Flask(__name__)
app.secret_key = 'calo_balance_sync_secret_key'

# Add custom JSON filter for templates
@app.template_filter('tojsonfilter')
def to_json_filter(obj):
    """Convert Python object to JSON for use in templates."""
    try:
        # Handle numpy arrays and pandas data types
        if hasattr(obj, 'tolist'):
            obj = obj.tolist()
        elif hasattr(obj, '__call__'):
            # If it's a callable (function/method), return empty list
            return json.dumps([])
        
        # Convert any numpy/pandas numeric types to native Python types
        if hasattr(obj, 'item'):
            obj = obj.item()
        
        return json.dumps(obj, default=str)
    except (TypeError, ValueError) as e:
        # If serialization fails, return empty array or string
        logging.warning(f"JSON serialization failed for {type(obj)}: {e}")
        return json.dumps([]) if isinstance(obj, (list, tuple)) else json.dumps("")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize components
log_parser = LogParser()
balance_tracker = BalanceTracker()
analyzer = DataAnalyzer()
report_generator = ReportGenerator()

# Global variables to store analysis results
current_data = {}

@app.route('/')
def index():
    """Main dashboard page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and processing."""
    try:
        if 'logfile' not in request.files:
            flash('No file selected')
            return redirect(url_for('index'))
        
        file = request.files['logfile']
        if file.filename == '':
            flash('No file selected')
            return redirect(url_for('index'))
        
        # Validate file type
        allowed_extensions = {'.log', '.txt', '.gz', '.zip', '.doc', '.docx'}
        file_extension = os.path.splitext(file.filename.lower())[1]
        
        if file_extension not in allowed_extensions:
            flash(f'Unsupported file type: {file_extension}. Supported formats: {", ".join(allowed_extensions)}')
            return redirect(url_for('index'))
        
        # Save uploaded file
        upload_dir = os.path.join('data', 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        filename = f"uploaded_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)
        
        logger.info(f"Processing uploaded file: {filename} (type: {file_extension})")
        
        # Process the file - check if it's a zip archive with multiple files
        if file_extension == '.zip':
            # Check if zip contains multiple .gz files (batch processing)
            df = log_parser.parse_zip_archive(filepath)
            processing_method = "zip archive with multiple files"
        else:
            # Single file processing
            df = log_parser.parse_log_file(filepath)
            processing_method = "single file"
        
        logger.info(f"Processing method: {processing_method}")
        
        if df.empty:
            # Check if it's a file with skip messages
            try:
                if filepath.endswith('.gz'):
                    import gzip
                    with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                elif filepath.endswith('.zip'):
                    # For zip files, we already handled this in parse_zip_archive
                    content = "zip archive processed"
                else:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                
                if "skipping the balance sync" in content.lower():
                    flash(f'The {file_extension} file contains balance sync skip messages but no actual transaction data. Please upload a file with payment/charge transactions.')
                else:
                    flash(f'No valid log entries found in the {file_extension} file. Please check the file format and content.')
            except:
                flash(f'No valid log entries found in the {file_extension} file. Please check the file format and content.')
            return redirect(url_for('index'))
        
        # Track balances
        df_with_balances = balance_tracker.process_transactions(df)
        
        # Generate analysis
        summary_stats = analyzer.generate_summary_stats(df_with_balances)
        subscriber_analysis = analyzer.analyze_subscriber_behavior(df_with_balances)
        anomalies = balance_tracker.detect_anomalies(df_with_balances)
        trends = analyzer.detect_trends(df_with_balances)
        visualizations = analyzer.generate_visualizations(df_with_balances)
        
        # Store results globally
        global current_data
        current_data = {
            'dataframe': df_with_balances,
            'summary_stats': summary_stats,
            'subscriber_analysis': subscriber_analysis,
            'anomalies': anomalies,
            'trends': trends,
            'visualizations': visualizations,
            'filename': filename
        }
        
        flash(f'Successfully processed {len(df_with_balances)} transactions from {file_extension} file')
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        flash(f'Error processing file: {str(e)}')
        return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    """Display analysis dashboard."""
    if not current_data:
        flash('Please upload a log file first')
        return redirect(url_for('index'))
    
    return render_template('dashboard.html', 
                         summary=current_data['summary_stats'],
                         visualizations=current_data['visualizations'],
                         anomalies=current_data['anomalies'][:10])  # Show first 10 anomalies

@app.route('/subscribers')
def subscribers():
    """Display subscriber analysis."""
    if not current_data:
        flash('Please upload a log file first')
        return redirect(url_for('index'))
    
    return render_template('subscribers.html',
                         subscriber_analysis=current_data['subscriber_analysis'])

@app.route('/api/summary')
def api_summary():
    """API endpoint for summary data."""
    if not current_data:
        return jsonify({'error': 'No data available'}), 404
    
    return jsonify(current_data['summary_stats'])

@app.route('/api/visualizations')
def api_visualizations():
    """API endpoint for visualization data."""
    if not current_data:
        return jsonify({'error': 'No data available'}), 404
    
    return jsonify(current_data['visualizations'])

@app.route('/download/excel')
def download_excel():
    """Generate and download Excel report."""
    if not current_data:
        flash('Please upload a log file first')
        return redirect(url_for('index'))
    
    try:
        filepath = report_generator.generate_excel_report(
            current_data['dataframe'],
            current_data['summary_stats'],
            current_data['subscriber_analysis'],
            current_data['anomalies']
        )
        
        if filepath and os.path.exists(filepath):
            return send_file(filepath, as_attachment=True, 
                           download_name=f"balance_report_{datetime.now().strftime('%Y%m%d')}.xlsx")
        else:
            flash('Error generating Excel report')
            return redirect(url_for('dashboard'))
            
    except Exception as e:
        logger.error(f"Error generating Excel report: {str(e)}")
        flash('Error generating Excel report')
        return redirect(url_for('dashboard'))

@app.route('/download/json')
def download_json():
    """Generate and download JSON report."""
    if not current_data:
        flash('Please upload a log file first')
        return redirect(url_for('index'))
    
    try:
        filepath = report_generator.generate_json_report(
            current_data['dataframe'],
            current_data['summary_stats'],
            current_data['subscriber_analysis'],
            current_data['anomalies']
        )
        
        if filepath and os.path.exists(filepath):
            return send_file(filepath, as_attachment=True,
                           download_name=f"balance_report_{datetime.now().strftime('%Y%m%d')}.json")
        else:
            flash('Error generating JSON report')
            return redirect(url_for('dashboard'))
            
    except Exception as e:
        logger.error(f"Error generating JSON report: {str(e)}")
        flash('Error generating JSON report')
        return redirect(url_for('dashboard'))

@app.route('/process_sample')
def process_sample():
    """Process sample data for demonstration."""
    try:
        # Create sample data
        sample_data = create_sample_data()
        
        # Process sample data
        df_with_balances = balance_tracker.process_transactions(sample_data)
        
        # Generate analysis
        summary_stats = analyzer.generate_summary_stats(df_with_balances)
        subscriber_analysis = analyzer.analyze_subscriber_behavior(df_with_balances)
        anomalies = balance_tracker.detect_anomalies(df_with_balances)
        trends = analyzer.detect_trends(df_with_balances)
        visualizations = analyzer.generate_visualizations(df_with_balances)
        
        # Store results globally
        global current_data
        current_data = {
            'dataframe': df_with_balances,
            'summary_stats': summary_stats,
            'subscriber_analysis': subscriber_analysis,
            'anomalies': anomalies,
            'trends': trends,
            'visualizations': visualizations,
            'filename': 'sample_data.log'
        }
        
        flash('Sample data processed successfully')
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        logger.error(f"Error processing sample data: {str(e)}")
        flash('Error processing sample data')
        return redirect(url_for('index'))

def create_sample_data():
    """Create sample transaction data for demonstration."""
    import random
    from datetime import timedelta
    
    # Sample data generation
    subscribers = ['sub_001', 'sub_002', 'sub_003', 'sub_004', 'sub_005']
    transaction_types = ['credit', 'debit', 'payment', 'charge']
    
    sample_transactions = []
    base_time = datetime.now() - timedelta(days=30)
    
    for i in range(200):
        transaction = {
            'timestamp': base_time + timedelta(hours=random.randint(0, 720)),
            'request_id': f'req_{i:04d}',
            'message_id': f'msg_{i:04d}',
            'subscriber_id': random.choice(subscribers),
            'transaction_type': random.choice(transaction_types),
            'amount': round(random.uniform(10, 500), 2),
            'status': random.choice(['success', 'success', 'success', 'failed']),  # 75% success rate
            'potential_overdraft': False,
            'duration_ms': random.uniform(50, 200),
            'operation': random.choice(['payment', 'charge', 'balance_sync']),
            'raw_log': f'Sample log entry {i}'
        }
        
        sample_transactions.append(transaction)
    
    return pd.DataFrame(sample_transactions)

if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('data/uploads', exist_ok=True)
    os.makedirs('data/logs', exist_ok=True)
    os.makedirs('reports', exist_ok=True)
    
    # Run the application
    app.run(host='0.0.0.0', port=9000, debug=True)
