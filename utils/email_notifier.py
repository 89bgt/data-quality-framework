"""
Professional Email Notification System for Data Quality Reports
"""

import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime
from typing import List, Dict, Any, Optional
from .helpers import calculate_test_statistics

logger = logging.getLogger(__name__)

class DataQualityEmailNotifier:
    """Professional email notifier for data quality reports"""
    
    def __init__(self, smtp_config: Dict[str, Any]):
        """
        Initialize email notifier with SMTP configuration
        
        Args:
            smtp_config: Dictionary containing SMTP settings
        """
        self.smtp_server = smtp_config.get('server', 'smtp.gmail.com')
        self.smtp_port = smtp_config.get('port', 465)
        self.username = smtp_config.get('username')
        self.password = smtp_config.get('password')
        self.sender_name = smtp_config.get('sender_name', 'Data Quality System')
        
    def _prepare_email_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """Prepare statistics for email using existing helper function"""
        # Use existing statistics calculation
        base_stats = calculate_test_statistics(results)
        
        # Calculate totals for email
        total_tests = base_stats['total_custom_tests'] + base_stats['total_gx_tests']
        passed_tests = base_stats['passed_custom_tests'] + base_stats['passed_gx_tests']
        failed_tests = total_tests - passed_tests
        
        # Organize by data quality dimensions based on test types
        dimensions = {
            'completeness': {'passed': 0, 'total': 0},
            'consistency': {'passed': 0, 'total': 0},
            'timeliness': {'passed': 0, 'total': 0},
            'uniqueness': {'passed': 0, 'total': 0}
        }
        
        # Map test types to dimensions
        dimension_mapping = {
            'null_columns_check': 'completeness',
            'row_count_check': 'completeness',
            'row_count_comparison': 'completeness',
            'schema_presence_check': 'consistency',
            'schema_types_check': 'consistency',
            'date_insertion_freshness_check': 'timeliness',
            'row_uniqueness_check': 'uniqueness'
        }
        
        # Count tests by dimension
        for test_type, test_stats in base_stats['test_types'].items():
            dimension = dimension_mapping.get(test_type, 'completeness')  # Default to completeness
            dimensions[dimension]['total'] += test_stats['total']
            dimensions[dimension]['passed'] += test_stats['passed']
        
        # Collect failed test details
        failed_test_details = []
        for result in results:
            if result.get('status') == 'SUCCESS':
                table_key = f"{result.get('database', '')}.{result.get('table', '')}"
                for test in result.get('custom_tests', []):
                    if test and not test.get('passed', True):
                        failed_test_details.append({
                            'table': table_key,
                            'test': test.get('test_name', 'Unknown'),
                            'details': test.get('details', ''),
                            'partition': result.get('partition', 'N/A')
                        })
        
        return {
            'total_tables': base_stats['total_tables'],
            'successful_tables': base_stats['successful_tables'],
            'failed_tables': base_stats['failed_tables'],
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'completeness_tests': dimensions['completeness'],
            'consistency_tests': dimensions['consistency'],
            'timeliness_tests': dimensions['timeliness'],
            'uniqueness_tests': dimensions['uniqueness'],
            'failed_test_details': failed_test_details
        }
    
    def _get_overall_status(self, stats: Dict[str, Any]) -> tuple:
        """Determine overall status and emoji"""
        if stats['total_tests'] == 0:
            return "🔴 CRITICAL", "No tests executed"
        
        pass_rate = (stats['passed_tests'] / stats['total_tests']) * 100
        
        if pass_rate == 100:
            return "🟢 EXCELLENT", f"{pass_rate:.1f}% pass rate"
        elif pass_rate >= 90:
            return "🟡 WARNING", f"{pass_rate:.1f}% pass rate"
        elif pass_rate >= 70:
            return "🟠 NEEDS ATTENTION", f"{pass_rate:.1f}% pass rate"
        else:
            return "🔴 CRITICAL", f"{pass_rate:.1f}% pass rate"
    
    def _create_email_body(self, stats: Dict[str, Any], report_filename: str, timestamp: str) -> str:
        """Create professional email body with executive summary"""
        status, status_desc = self._get_overall_status(stats)
        
        # Calculate dimension pass rates
        def calc_rate(dimension):
            total = stats[f'{dimension}_tests']['total']
            passed = stats[f'{dimension}_tests']['passed']
            return (passed / total * 100) if total > 0 else 0, passed, total
        
        completeness_rate, comp_pass, comp_total = calc_rate('completeness')
        consistency_rate, cons_pass, cons_total = calc_rate('consistency')
        timeliness_rate, time_pass, time_total = calc_rate('timeliness')
        uniqueness_rate, uniq_pass, uniq_total = calc_rate('uniqueness')
        
        # Create action items
        action_items = []
        if stats['failed_tests'] > 0:
            action_items.append(f"• Review {stats['failed_tests']} failed test(s)")
        if timeliness_rate < 100 and time_total > 0:
            action_items.append(f"• Address {time_total - time_pass} data freshness issue(s)")
        if consistency_rate < 100 and cons_total > 0:
            action_items.append(f"• Fix {cons_total - cons_pass} schema inconsistency/ies")
        
        body = f"""Dear Data Quality Team,

Please find attached the latest Data Quality Assessment Report generated on {timestamp}.

📊 EXECUTIVE SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Overall Status: {status} ({status_desc})

📈 KEY METRICS:
• Total Tables Processed: {stats['total_tables']}
• Successfully Processed: {stats['successful_tables']}
• Failed to Process: {stats['failed_tables']}
• Total Quality Tests: {stats['total_tests']}
• Tests Passed: {stats['passed_tests']}

📋 DETAILED ANALYSIS:
The attached PDF report contains comprehensive analysis organized by database and data quality dimensions:
- Completeness: Null value and row count validation
- Consistency: Schema and data type validation 
- Timeliness: Data freshness verification
- Uniqueness: Duplicate row detection"""

        if action_items:
            body += f"""

🚨 ACTION REQUIRED:

{chr(10).join(action_items)}"""

        body += f"""

📎 ATTACHMENTS:
• {os.path.basename(report_filename)} - Comprehensive Data Quality Report

For technical questions or support, please contact the Data Engineering team.

Best regards,
Data Quality Monitoring System

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This is an automated message from the Data Quality Framework.
Report generated at: {timestamp}"""

        return body
    
    def _create_subject_line(self, stats: Dict[str, Any], timestamp: str) -> str:
        """Create dynamic subject line based on results"""
        status_full, status_desc = self._get_overall_status(stats)
        status_emoji = status_full.split()[0]  # Extract emoji
        status_text = status_full.split()[1]   # Extract status text
        
        # Parse timestamp to get date
        try:
            if ' ' in timestamp:
                date_str = timestamp.split()[0]
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
        except:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        # Calculate processed vs total tables
        processed_tables = stats['successful_tables'] + stats['failed_tables']
        total_tables = stats['total_tables']
        
        return f"Data Quality Report - {status_emoji} {status_text} - {date_str} ({processed_tables}/{total_tables} tables)"
    
    def send_report(self, 
                   results: List[Dict], 
                   report_file_path: str, 
                   recipients: List[str],
                   cc_recipients: Optional[List[str]] = None,
                   bcc_recipients: Optional[List[str]] = None) -> bool:
        """
        Send data quality report via email
        
        Args:
            results: List of test results
            report_file_path: Path to the PDF report file
            recipients: List of recipient email addresses
            cc_recipients: List of CC recipient email addresses
            bcc_recipients: List of BCC recipient email addresses
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            # Calculate statistics using existing helper
            stats = self._prepare_email_statistics(results)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = f"{self.sender_name} <{self.username}>"
            msg['To'] = ', '.join(recipients)
            
            if cc_recipients:
                msg['Cc'] = ', '.join(cc_recipients)
            
            msg['Subject'] = self._create_subject_line(stats, timestamp)
            
            # Create email body
            body = self._create_email_body(stats, report_file_path, timestamp)
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach PDF report
            if os.path.exists(report_file_path):
                with open(report_file_path, 'rb') as attachment:
                    part = MIMEApplication(
                        attachment.read(),
                        Name=os.path.basename(report_file_path)
                    )
                    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(report_file_path)}"'
                    msg.attach(part)
            else:
                logger.warning(f"Report file not found: {report_file_path}")
                return False
            
            # Send email
            all_recipients = recipients[:]
            if cc_recipients:
                all_recipients.extend(cc_recipients)
            if bcc_recipients:
                all_recipients.extend(bcc_recipients)
            
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.username, self.password)
                server.send_message(msg, to_addrs=all_recipients)
            
            logger.info(f"Data quality report email sent successfully to {len(all_recipients)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False
    
    def test_connection(self) -> bool:
        """Test SMTP connection and credentials"""
        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.username, self.password)
            logger.info("Email connection test successful")
            return True
        except Exception as e:
            logger.error(f"Email connection test failed: {str(e)}")
            return False
