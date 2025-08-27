#!/usr/bin/env python3
"""
Calo Balance Sync Log Analyzer
Main application entry point for command-line usage.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from log_parser import LogParser
from balance_tracker import BalanceTracker
from analyzer import DataAnalyzer
from report_generator import ReportGenerator

def setup_logging(level=logging.INFO):
    """Setup logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('analyzer.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    """Main application function."""
    parser = argparse.ArgumentParser(description='Analyze Calo balance sync logs')
    parser.add_argument('input_file', help='Path to log file to analyze (.log, .txt, .gz, .zip, .doc, .docx)')
    parser.add_argument('--output-dir', '-o', default='reports', 
                       help='Output directory for reports (default: reports)')
    parser.add_argument('--format', '-f', choices=['excel', 'json', 'html', 'all'], 
                       default='all', help='Output format (default: all)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Calo Balance Sync Log Analyzer")
    
    try:
        # Validate input file
        if not os.path.exists(args.input_file):
            logger.error(f"Input file not found: {args.input_file}")
            return 1
        
        # Validate file type
        file_extension = os.path.splitext(args.input_file.lower())[1]
        supported_extensions = {'.log', '.txt', '.gz', '.zip', '.doc', '.docx'}
        
        if file_extension not in supported_extensions:
            logger.error(f"Unsupported file type: {file_extension}")
            logger.error(f"Supported formats: {', '.join(supported_extensions)}")
            return 1
        
        # Create output directory
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Initialize components
        log_parser = LogParser()
        balance_tracker = BalanceTracker()
        analyzer = DataAnalyzer()
        report_generator = ReportGenerator(args.output_dir)
        
        logger.info(f"Processing {file_extension} file: {args.input_file}")
        
        # Parse logs
        df = log_parser.parse_log_file(args.input_file)
        if df.empty:
            logger.error("No valid log entries found")
            return 1
        
        logger.info(f"Parsed {len(df)} log entries from {file_extension} file")
        
        # Track balances
        df_with_balances = balance_tracker.process_transactions(df)
        logger.info("Balance tracking completed")
        
        # Generate analysis
        summary_stats = analyzer.generate_summary_stats(df_with_balances)
        subscriber_analysis = analyzer.analyze_subscriber_behavior(df_with_balances)
        anomalies = balance_tracker.detect_anomalies(df_with_balances)
        
        logger.info(f"Analysis completed: {len(anomalies)} anomalies detected")
        
        # Generate reports
        reports_generated = []
        
        if args.format in ['excel', 'all']:
            excel_path = report_generator.generate_excel_report(
                df_with_balances, summary_stats, subscriber_analysis, anomalies
            )
            if excel_path:
                reports_generated.append(excel_path)
                logger.info(f"Excel report generated: {excel_path}")
        
        if args.format in ['json', 'all']:
            json_path = report_generator.generate_json_report(
                df_with_balances, summary_stats, subscriber_analysis, anomalies
            )
            if json_path:
                reports_generated.append(json_path)
                logger.info(f"JSON report generated: {json_path}")
        
        if args.format in ['html', 'all']:
            html_path = report_generator.generate_html_summary(summary_stats)
            if html_path:
                reports_generated.append(html_path)
                logger.info(f"HTML summary generated: {html_path}")
        
        # Print summary
        print("\n" + "="*60)
        print("ANALYSIS SUMMARY")
        print("="*60)
        print(f"Input file: {args.input_file} ({file_extension})")
        print(f"Total transactions: {summary_stats['data_overview']['total_transactions']:,}")
        print(f"Unique subscribers: {summary_stats['data_overview']['unique_subscribers']}")
        print(f"Subscribers with overdrafts: {summary_stats['balance_analysis']['subscribers_with_overdrafts']}")
        print(f"Total overdraft instances: {summary_stats['balance_analysis']['total_overdraft_instances']}")
        print(f"Anomalies detected: {len(anomalies)}")
        print(f"\nReports generated:")
        for report in reports_generated:
            print(f"  - {report}")
        print("="*60)
        
        logger.info("Analysis completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
