#!/usr/bin/env python3
"""
monitor_extraction.py  –  Monitoring and alerting for HCFA-1500 extraction system

Features:
• Real-time performance monitoring
• Failure pattern analysis
• Quality metrics tracking
• Automated alerts for system issues
• Historical trend analysis
• Database health monitoring

Required:
    pip install sqlite3 pandas matplotlib seaborn
"""

import os
import sys
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configure paths
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

# Database path
DB_PATH = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\monolith.db"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / f"extraction_monitor_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ExtractionMetrics:
    """Metrics for extraction performance monitoring."""
    total_bills: int = 0
    successful_extractions: int = 0
    failed_extractions: int = 0
    validation_failures: int = 0
    database_errors: int = 0
    api_errors: int = 0
    processing_time_avg: float = 0.0
    success_rate: float = 0.0
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        
        if self.total_bills > 0:
            self.success_rate = (self.successful_extractions / self.total_bills) * 100

@dataclass
class AlertThresholds:
    """Configurable thresholds for alerts."""
    min_success_rate: float = 85.0
    max_processing_time: float = 300.0  # 5 minutes
    max_consecutive_failures: int = 5
    max_validation_failures: float = 20.0  # percentage
    max_api_errors: float = 10.0  # percentage

class ExtractionMonitor:
    """Monitor and analyze HCFA-1500 extraction performance."""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.thresholds = AlertThresholds()
        self.metrics_history: List[ExtractionMetrics] = []
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn
    
    def collect_current_metrics(self, hours_back: int = 24) -> ExtractionMetrics:
        """Collect current extraction metrics from the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get time range
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                
                # Total bills processed
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM ProviderBill 
                    WHERE status = 'RECEIVED' 
                    AND updated_at >= ?
                """, (cutoff_time,))
                total_bills = cursor.fetchone()['count']
                
                # Successful extractions (bills with line items)
                cursor.execute("""
                    SELECT COUNT(DISTINCT pb.id) as count
                    FROM ProviderBill pb
                    JOIN BillLineItem bli ON pb.id = bli.provider_bill_id
                    WHERE pb.status = 'RECEIVED' 
                    AND pb.updated_at >= ?
                """, (cutoff_time,))
                successful_extractions = cursor.fetchone()['count']
                
                # Failed extractions (bills with errors)
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM ProviderBill 
                    WHERE last_error IS NOT NULL 
                    AND last_error != ''
                    AND updated_at >= ?
                """, (cutoff_time,))
                failed_extractions = cursor.fetchone()['count']
                
                # Validation failures (bills with specific error patterns)
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM ProviderBill 
                    WHERE last_error LIKE '%validation%' 
                    OR last_error LIKE '%service line%'
                    OR last_error LIKE '%CPT%'
                    AND updated_at >= ?
                """, (cutoff_time,))
                validation_failures = cursor.fetchone()['count']
                
                # Database errors
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM ProviderBill 
                    WHERE last_error LIKE '%database%' 
                    OR last_error LIKE '%SQL%'
                    AND updated_at >= ?
                """, (cutoff_time,))
                database_errors = cursor.fetchone()['count']
                
                # API errors
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM ProviderBill 
                    WHERE last_error LIKE '%API%' 
                    OR last_error LIKE '%OpenAI%'
                    OR last_error LIKE '%rate limit%'
                    AND updated_at >= ?
                """, (cutoff_time,))
                api_errors = cursor.fetchone()['count']
                
                # Calculate average processing time (if available)
                cursor.execute("""
                    SELECT AVG(processing_time) as avg_time
                    FROM ProviderBill 
                    WHERE processing_time IS NOT NULL 
                    AND updated_at >= ?
                """, (cutoff_time,))
                result = cursor.fetchone()
                processing_time_avg = result['avg_time'] if result['avg_time'] else 0.0
                
                return ExtractionMetrics(
                    total_bills=total_bills,
                    successful_extractions=successful_extractions,
                    failed_extractions=failed_extractions,
                    validation_failures=validation_failures,
                    database_errors=database_errors,
                    api_errors=api_errors,
                    processing_time_avg=processing_time_avg
                )
                
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            return ExtractionMetrics()
    
    def analyze_error_patterns(self, hours_back: int = 24) -> Dict[str, Any]:
        """Analyze patterns in extraction errors."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                
                # Get all errors
                cursor.execute("""
                    SELECT last_error, COUNT(*) as count
                    FROM ProviderBill 
                    WHERE last_error IS NOT NULL 
                    AND last_error != ''
                    AND updated_at >= ?
                    GROUP BY last_error
                    ORDER BY count DESC
                    LIMIT 10
                """, (cutoff_time,))
                
                error_patterns = []
                for row in cursor.fetchall():
                    error_patterns.append({
                        'error': row['last_error'],
                        'count': row['count']
                    })
                
                # Analyze error categories
                error_categories = {
                    'validation_errors': 0,
                    'api_errors': 0,
                    'database_errors': 0,
                    'service_line_errors': 0,
                    'other_errors': 0
                }
                
                for pattern in error_patterns:
                    error = pattern['error'].lower()
                    if any(keyword in error for keyword in ['validation', 'service line', 'cpt']):
                        error_categories['validation_errors'] += pattern['count']
                    elif any(keyword in error for keyword in ['api', 'openai', 'rate limit']):
                        error_categories['api_errors'] += pattern['count']
                    elif any(keyword in error for keyword in ['database', 'sql']):
                        error_categories['database_errors'] += pattern['count']
                    elif 'service line' in error:
                        error_categories['service_line_errors'] += pattern['count']
                    else:
                        error_categories['other_errors'] += pattern['count']
                
                return {
                    'error_patterns': error_patterns,
                    'error_categories': error_categories
                }
                
        except Exception as e:
            logger.error(f"Error analyzing error patterns: {e}")
            return {'error_patterns': [], 'error_categories': {}}
    
    def check_alert_conditions(self, metrics: ExtractionMetrics) -> List[str]:
        """Check for alert conditions based on current metrics."""
        alerts = []
        
        # Success rate alert
        if metrics.success_rate < self.thresholds.min_success_rate:
            alerts.append(f"LOW SUCCESS RATE: {metrics.success_rate:.1f}% (threshold: {self.thresholds.min_success_rate}%)")
        
        # Processing time alert
        if metrics.processing_time_avg > self.thresholds.max_processing_time:
            alerts.append(f"HIGH PROCESSING TIME: {metrics.processing_time_avg:.1f}s (threshold: {self.thresholds.max_processing_time}s)")
        
        # Validation failures alert
        if metrics.total_bills > 0:
            validation_rate = (metrics.validation_failures / metrics.total_bills) * 100
            if validation_rate > self.thresholds.max_validation_failures:
                alerts.append(f"HIGH VALIDATION FAILURES: {validation_rate:.1f}% (threshold: {self.thresholds.max_validation_failures}%)")
        
        # API errors alert
        if metrics.total_bills > 0:
            api_error_rate = (metrics.api_errors / metrics.total_bills) * 100
            if api_error_rate > self.thresholds.max_api_errors:
                alerts.append(f"HIGH API ERRORS: {api_error_rate:.1f}% (threshold: {self.thresholds.max_api_errors}%)")
        
        # Consecutive failures alert
        if metrics.failed_extractions >= self.thresholds.max_consecutive_failures:
            alerts.append(f"CONSECUTIVE FAILURES: {metrics.failed_extractions} (threshold: {self.thresholds.max_consecutive_failures})")
        
        return alerts
    
    def generate_performance_report(self, hours_back: int = 24) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        metrics = self.collect_current_metrics(hours_back)
        error_analysis = self.analyze_error_patterns(hours_back)
        alerts = self.check_alert_conditions(metrics)
        
        # Store metrics in history
        self.metrics_history.append(metrics)
        
        # Keep only last 30 days of history
        cutoff = datetime.now() - timedelta(days=30)
        self.metrics_history = [m for m in self.metrics_history if m.timestamp > cutoff]
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'time_period_hours': hours_back,
            'metrics': {
                'total_bills': metrics.total_bills,
                'successful_extractions': metrics.successful_extractions,
                'failed_extractions': metrics.failed_extractions,
                'validation_failures': metrics.validation_failures,
                'database_errors': metrics.database_errors,
                'api_errors': metrics.api_errors,
                'processing_time_avg': metrics.processing_time_avg,
                'success_rate': metrics.success_rate
            },
            'error_analysis': error_analysis,
            'alerts': alerts,
            'status': 'HEALTHY' if not alerts else 'ISSUES_DETECTED'
        }
        
        return report
    
    def create_performance_charts(self, output_dir: str = None) -> List[str]:
        """Create performance visualization charts."""
        if output_dir is None:
            output_dir = PROJECT_ROOT / "reports" / "extraction_performance"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.metrics_history:
            logger.warning("No metrics history available for charts")
            return []
        
        chart_files = []
        
        # Convert history to DataFrame
        df = pd.DataFrame([
            {
                'timestamp': m.timestamp,
                'total_bills': m.total_bills,
                'successful': m.successful_extractions,
                'failed': m.failed_extractions,
                'success_rate': m.success_rate,
                'processing_time': m.processing_time_avg
            }
            for m in self.metrics_history
        ])
        
        if df.empty:
            return []
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # 1. Success Rate Trend
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['success_rate'], marker='o', linewidth=2)
        plt.axhline(y=self.thresholds.min_success_rate, color='red', linestyle='--', 
                   label=f'Threshold ({self.thresholds.min_success_rate}%)')
        plt.title('Extraction Success Rate Trend', fontsize=14, fontweight='bold')
        plt.xlabel('Time')
        plt.ylabel('Success Rate (%)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        chart_file = output_dir / f"success_rate_trend_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        plt.savefig(chart_file, dpi=300, bbox_inches='tight')
        plt.close()
        chart_files.append(str(chart_file))
        
        # 2. Processing Volume
        plt.figure(figsize=(12, 6))
        plt.bar(df['timestamp'], df['total_bills'], alpha=0.7, label='Total Bills')
        plt.bar(df['timestamp'], df['successful'], alpha=0.9, label='Successful')
        plt.title('Processing Volume Over Time', fontsize=14, fontweight='bold')
        plt.xlabel('Time')
        plt.ylabel('Number of Bills')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        chart_file = output_dir / f"processing_volume_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        plt.savefig(chart_file, dpi=300, bbox_inches='tight')
        plt.close()
        chart_files.append(str(chart_file))
        
        # 3. Error Distribution (if we have recent error analysis)
        if self.metrics_history:
            latest_metrics = self.metrics_history[-1]
            if latest_metrics.total_bills > 0:
                error_data = {
                    'Successful': latest_metrics.successful_extractions,
                    'Validation Failures': latest_metrics.validation_failures,
                    'API Errors': latest_metrics.api_errors,
                    'Database Errors': latest_metrics.database_errors,
                    'Other Failures': latest_metrics.failed_extractions - latest_metrics.validation_failures - latest_metrics.api_errors - latest_metrics.database_errors
                }
                
                # Remove zero values
                error_data = {k: v for k, v in error_data.items() if v > 0}
                
                if error_data:
                    plt.figure(figsize=(10, 8))
                    plt.pie(error_data.values(), labels=error_data.keys(), autopct='%1.1f%%', startangle=90)
                    plt.title('Error Distribution (Last 24 Hours)', fontsize=14, fontweight='bold')
                    plt.axis('equal')
                    
                    chart_file = output_dir / f"error_distribution_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
                    plt.savefig(chart_file, dpi=300, bbox_inches='tight')
                    plt.close()
                    chart_files.append(str(chart_file))
        
        logger.info(f"Created {len(chart_files)} performance charts in {output_dir}")
        return chart_files
    
    def save_report(self, report: Dict[str, Any], output_dir: str = None) -> str:
        """Save performance report to file."""
        if output_dir is None:
            output_dir = PROJECT_ROOT / "reports" / "extraction_performance"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = output_dir / f"extraction_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Saved performance report to {report_file}")
        return str(report_file)
    
    def send_alerts(self, alerts: List[str], report: Dict[str, Any]) -> bool:
        """Send alerts for critical issues."""
        if not alerts:
            return True
        
        try:
            # Log alerts
            for alert in alerts:
                logger.warning(f"ALERT: {alert}")
            
            # Save alert report
            alert_report = {
                'timestamp': datetime.now().isoformat(),
                'alerts': alerts,
                'metrics': report['metrics'],
                'status': 'CRITICAL'
            }
            
            alert_file = PROJECT_ROOT / "logs" / f"extraction_alerts_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(alert_file, 'w') as f:
                json.dump(alert_report, f, indent=2, default=str)
            
            logger.warning(f"Alert report saved to {alert_file}")
            
            # TODO: Add email/Slack notifications here
            # For now, just log the alerts
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending alerts: {e}")
            return False

def main():
    """Main monitoring function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor HCFA-1500 extraction performance")
    parser.add_argument("--hours", type=int, default=24, help="Hours to look back for metrics")
    parser.add_argument("--charts", action="store_true", help="Generate performance charts")
    parser.add_argument("--alerts", action="store_true", help="Send alerts for issues")
    
    args = parser.parse_args()
    
    monitor = ExtractionMonitor()
    
    # Generate report
    report = monitor.generate_performance_report(args.hours)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"EXTRACTION PERFORMANCE REPORT")
    print(f"{'='*60}")
    print(f"Time Period: Last {args.hours} hours")
    print(f"Total Bills: {report['metrics']['total_bills']}")
    print(f"Successful: {report['metrics']['successful_extractions']}")
    print(f"Failed: {report['metrics']['failed_extractions']}")
    print(f"Success Rate: {report['metrics']['success_rate']:.1f}%")
    print(f"Avg Processing Time: {report['metrics']['processing_time_avg']:.1f}s")
    print(f"Status: {report['status']}")
    
    if report['alerts']:
        print(f"\n⚠️  ALERTS:")
        for alert in report['alerts']:
            print(f"   • {alert}")
    
    # Generate charts if requested
    if args.charts:
        chart_files = monitor.create_performance_charts()
        if chart_files:
            print(f"\n📊 Charts generated: {len(chart_files)} files")
    
    # Send alerts if requested
    if args.alerts and report['alerts']:
        monitor.send_alerts(report['alerts'], report)
        print(f"\n🚨 Alerts sent for {len(report['alerts'])} issues")
    
    # Save report
    report_file = monitor.save_report(report)
    print(f"\n📄 Report saved: {report_file}")
    
    print(f"{'='*60}")

if __name__ == "__main__":
    main() 