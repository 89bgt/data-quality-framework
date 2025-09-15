"""
PDF report generation module
"""

import os
import logging
import tempfile
from datetime import datetime
from typing import List, Dict, Any, Optional

import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus.flowables import HRFlowable

from config import PDF_CONFIG, OUTPUT_CONFIG

logger = logging.getLogger(__name__)

class PDFReportGenerator:
    """Generates professional PDF reports for data quality results"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize PDF generator
        
        Args:
            config (Dict[str, Any], optional): PDF configuration
        """
        self.config = config or PDF_CONFIG
        
    def _create_summary_chart(self, results: List[Dict[str, Any]]) -> Optional[str]:
        """
        Create a summary chart for the PDF report
        
        Args:
            results (List[Dict[str, Any]]): Test results
            
        Returns:
            str: Path to chart image or None if failed
        """
        try:
            # Prepare data for the chart
            test_results = {'PASS': 0, 'FAIL': 0}
            for result in results:
                if result['status'] == 'SUCCESS':
                    for test in result.get('custom_tests', []):
                        if test and 'passed' in test:
                            if test['passed']:
                                test_results['PASS'] += 1
                            else:
                                test_results['FAIL'] += 1
            
            # Create pie chart
            labels = ['Passed', 'Failed']
            sizes = [test_results['PASS'], test_results['FAIL']]
            colors_list = ['#4CAF50', '#F44336']
            
            plt.figure(figsize=(6, 4))
            plt.pie(sizes, labels=labels, colors=colors_list, autopct='%1.1f%%', startangle=90)
            plt.axis('equal')
            plt.title('Test Results Summary')
            
            # Save the chart to a temporary file
            chart_path = os.path.join(tempfile.gettempdir(), 'test_results_chart.png')
            plt.savefig(chart_path, bbox_inches='tight')
            plt.close()
            
            return chart_path
            
        except Exception as e:
            logger.error(f"Error creating summary chart: {str(e)}")
            return None
    
    def _create_pdf_content(self, results: List[Dict[str, Any]], output_path: str) -> bool:
        """
        Generate professional PDF content organized by database
        
        Args:
            results (List[Dict[str, Any]]): Test results
            output_path (str): Output file path
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use portrait A4 for professional appearance
            doc = SimpleDocTemplate(
                output_path,
                pagesize=A4,
                rightMargin=30, leftMargin=30,
                topMargin=50, bottomMargin=50
            )
            
            # Define professional styles
            styles = getSampleStyleSheet()
            
            # Custom styles for professional appearance
            if 'ReportTitle' not in styles:
                styles.add(ParagraphStyle(
                    name='ReportTitle',
                    parent=styles['Heading1'],
                    fontSize=24,
                    fontName='Helvetica-Bold',
                    alignment=TA_CENTER,
                    spaceAfter=30,
                    textColor=colors.HexColor('#1f4e79')
                ))
                
            if 'ReportSubtitle' not in styles:
                styles.add(ParagraphStyle(
                    name='ReportSubtitle',
                    parent=styles['Normal'],
                    fontSize=12,
                    fontName='Helvetica',
                    alignment=TA_CENTER,
                    spaceAfter=20,
                    textColor=colors.HexColor('#666666')
                ))
                
            if 'DatabaseHeader' not in styles:
                styles.add(ParagraphStyle(
                    name='DatabaseHeader',
                    parent=styles['Heading2'],
                    fontSize=16,
                    fontName='Helvetica-Bold',
                    alignment=TA_LEFT,
                    spaceAfter=15,
                    spaceBefore=25,
                    textColor=colors.HexColor('#2c5aa0'),
                    borderWidth=1,
                    borderColor=colors.HexColor('#2c5aa0'),
                    borderPadding=8
                ))
                
            if 'TableHeader' not in styles:
                styles.add(ParagraphStyle(
                    name='TableHeader',
                    parent=styles['Heading3'],
                    fontSize=14,
                    fontName='Helvetica-Bold',
                    alignment=TA_LEFT,
                    spaceAfter=10,
                    spaceBefore=15,
                    textColor=colors.HexColor('#34495e')
                ))
                
            # Add a style for the details text with proper wrapping
            if 'DetailsText' not in styles:
                styles.add(ParagraphStyle(
                    name='DetailsText',
                    parent=styles['Normal'],
                    fontSize=9,
                    fontName='Helvetica',
                    alignment=TA_LEFT,
                    leading=11,
                    spaceAfter=6,
                    textColor=colors.HexColor('#2c3e50')
                ))
                
            if 'DimensionHeader' not in styles:
                styles.add(ParagraphStyle(
                    name='DimensionHeader',
                    parent=styles['Normal'],
                    fontSize=11,
                    fontName='Helvetica-Bold',
                    alignment=TA_LEFT,
                    spaceAfter=5,
                    spaceBefore=8,
                    textColor=colors.HexColor('#7f8c8d')
                ))
            
            # Create story (content)
            story = []
            
            # Professional header section
            title = self.config.get('title', 'Data Quality Assessment Report')
            story.append(Paragraph(title, styles['ReportTitle']))
            story.append(Paragraph(f"Generated on: {datetime.now().strftime('%B %d, %Y at %H:%M')}", styles['ReportSubtitle']))
            story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1f4e79')))
            story.append(Spacer(1, 20))
            
            # Executive Summary
            story.append(Paragraph("Executive Summary", styles['Heading2']))
            
            # Calculate overall statistics
            total_tables = len(results)
            successful_tables = len([r for r in results if r['status'] == 'SUCCESS'])
            failed_tables = total_tables - successful_tables
            
            # Calculate dimension statistics
            dimension_stats = {
                'completeness': {'total': 0, 'passed': 0},
                'consistency': {'total': 0, 'passed': 0},
                'timeliness': {'total': 0, 'passed': 0},
                'uniqueness': {'total': 0, 'passed': 0}
            }
            
            for result in results:
                if result['status'] == 'SUCCESS':
                    for test in result.get('custom_tests', []):
                        if test and test.get('dimension'):
                            dimension = test['dimension']
                            if dimension in dimension_stats:
                                dimension_stats[dimension]['total'] += 1
                                if test.get('passed', False):
                                    dimension_stats[dimension]['passed'] += 1
            
            # Add environment comparison section if we have comparison data
            has_comparisons = any(
                any(test.get('test_name') == 'row_count_comparison' 
                    for test in result.get('custom_tests', []))
                for result in results
            )
            
            if has_comparisons:
                story.append(Spacer(1, 20))
                story.append(Paragraph("🔄 DEV vs PROD Environment Comparison", styles['Heading2']))
                story.append(Spacer(1, 10))
                self._add_comparison_section(story, results, styles, detailed=True)
            
            # Summary table with professional styling
            summary_data = [
                ['Metric', 'Value', 'Status'],
                ['Total Tables Processed', str(total_tables), '✓' if successful_tables == total_tables else '⚠'],
                ['Successfully Processed', str(successful_tables), '✓' if successful_tables > 0 else '✗'],
                ['Failed to Process', str(failed_tables), '✓' if failed_tables == 0 else '✗'],
                ['', '', ''],  # Separator
                ['Data Quality Dimensions', '', ''],
            ]
            
            for dimension, stats in dimension_stats.items():
                if stats['total'] > 0:
                    percentage = (stats['passed'] / stats['total']) * 100
                    status = '✓' if percentage >= 90 else '⚠' if percentage >= 70 else '✗'
                    summary_data.append([
                        f"  {dimension.title()} Tests",
                        f"{stats['passed']}/{stats['total']} ({percentage:.1f}%)",
                        status
                    ])
            
            summary_table = Table(summary_data, colWidths=[250, 150, 80])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4e79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (2, 0), (2, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('TOPPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#ddd')),
                ('BACKGROUND', (0, 5), (-1, 5), colors.HexColor('#f8f9fa')),
                ('FONTNAME', (0, 5), (-1, 5), 'Helvetica-Bold'),
            ]))
            
            story.append(summary_table)
            story.append(Spacer(1, 30))
            
            # Detailed Results organized by database
            if self.config.get('include_detailed_results', True):
                story.append(PageBreak())
                story.append(Paragraph("Detailed Assessment Results", styles['Heading2']))
                story.append(Spacer(1, 20))
            
            # Group results by environment and database for better organization
            env_database_results = {}
            for result in results:
                if result['status'] == 'SUCCESS':
                    # Use the actual environment field from the result
                    env = result.get('environment', 'UNKNOWN')
                    db = result['database']
                    
                    if env not in env_database_results:
                        env_database_results[env] = {}
                    if db not in env_database_results[env]:
                        env_database_results[env][db] = []
                    env_database_results[env][db].append(result)
            
            # Process each environment (dynamically based on what's available)
            for environment in sorted(env_database_results.keys()):
                if env_database_results[environment]:  # Only show if we have data
                    # Environment header
                    story.append(Paragraph(f"🌐 {environment} Environment", styles['Heading2']))
                    story.append(Spacer(1, 15))
                    
                    # Process each database in this environment
                    for database, db_results in env_database_results[environment].items():
                        # Database header
                        story.append(Paragraph(f"Database: {database.upper()}", styles['DatabaseHeader']))
                        story.append(Spacer(1, 10))
                        
                        # Process each table in this database
                        for result in db_results:
                            table_name = result['table']
                            partition_info = result.get('partition', None)
                            
                            # Table header with professional styling
                            table_title = f"Table: {table_name}"
                            if partition_info:
                                table_title += f" (Partition: {partition_info})"
                            
                            story.append(Paragraph(table_title, styles['TableHeader']))
                            
                            # Find the latest date from timeliness checks
                            latest_date = None
                            for test in result.get('custom_tests', []):
                                if test.get('test_name') == 'date_insertion_freshness_check' and 'latest_datetime_found' in test:
                                    latest_date = test['latest_datetime_found']
                                    break
                            
                            # Table metadata
                            metadata_data = [
                                ['Row Count', f"{result['table_info']['row_count']:,}"],
                                ['Column Count', str(result['table_info']['column_count'])],
                                ['Assessment Date', datetime.now().strftime('%Y-%m-%d %H:%M')]
                            ]
                            
                            # Add latest date if available
                            if latest_date:
                                metadata_data.append(['Latest Data Date', latest_date])
                            
                            # Adjust column widths based on content
                            metadata_table = Table(metadata_data, colWidths=[150, 170])
                            metadata_table.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f8f9fa')),
                                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                                ('FONTSIZE', (0, 0), (-1, -1), 10),
                                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#ddd')),
                                ('TOPPADDING', (0, 0), (-1, -1), 6),
                                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                            ]))
                            
                            story.append(metadata_table)
                            story.append(Spacer(1, 15))
                            
                            # Organize tests by dimension
                            dimension_tests = {
                                'completeness': [],
                                'consistency': [],
                                'timeliness': [],
                                'uniqueness': []
                            }
                            
                            for test in result.get('custom_tests', []):
                                if test and test.get('dimension'):
                                    dimension = test['dimension']
                                    if dimension in dimension_tests:
                                        dimension_tests[dimension].append(test)
                            
                            # Create dimension results table
                            dimension_data = [['DQ Dimension', 'Test', 'Result', 'Details']]
                            
                            for dimension, tests in dimension_tests.items():
                                if tests:
                                    # Add dimension row
                                    dimension_data.append([
                                        Paragraph(f"<b>{dimension.upper()}</b>", styles['DimensionHeader']),
                                        '', '', ''
                                    ])
                                    
                                    # Add tests for this dimension
                                    for test in tests:
                                        status = "PASS" if test.get('passed', False) else "FAIL"
                                        # Get urgency color from config
                                        from config import URGENCY_CONFIG
                                        if status == 'PASS':
                                            color_name = URGENCY_CONFIG['urgency_levels']['PASS']['color']
                                        else:
                                            color_name = URGENCY_CONFIG['urgency_levels']['CRITICAL']['color']
                                        test_name = test.get('test_name', 'Test').replace('_check', '').replace('_', ' ').title()
                                        
                                        details = test.get('details', '')
                                        # Add specific details for different test types
                                        if test.get('test_name') == 'row_uniqueness_check':
                                            total_rows = test.get('total_rows', 0)
                                            duplicate_rows = test.get('duplicate_rows', 0)
                                            if duplicate_rows > 0:
                                                details = f"Found {duplicate_rows:,} duplicates ({test.get('duplicate_percentage', 0):.2f}%)"
                                        # Add schema presence details (missing/extra columns)
                                        elif test.get('test_name') == 'schema_presence_check':
                                            details_parts = []
                                            if 'missing_columns' in test and test['missing_columns']:
                                                missing = ", ".join(test['missing_columns'])
                                                details_parts.append(f"Missing columns: {missing}")
                                            if 'extra_columns' in test and test['extra_columns']:
                                                extra = ", ".join(test['extra_columns'])
                                                details_parts.append(f"Extra columns: {extra}")
                                            details = " | ".join(details_parts) if details_parts else "Schema presence validation passed"
                                        
                                        # Add schema types details (type mismatches)
                                        elif test.get('test_name') == 'schema_types_check':
                                            if 'type_mismatches' in test and test['type_mismatches']:
                                                type_issues = []
                                                for mismatch in test['type_mismatches']:
                                                    col = mismatch['column']
                                                    exp = mismatch['expected_type']
                                                    got = mismatch['actual_type']
                                                    type_issues.append(f"{col} (expected: {exp}, got: {got})")
                                                details = f"Type mismatches: {', '.join(type_issues)}"
                                            else:
                                                details = "Schema types validation passed"
                                        
                                        # Add row count check details with dynamic thresholds
                                        elif test.get('test_name') == 'row_count_check':
                                            actual_count = test.get('actual_row_count', 0)
                                            min_required = test.get('minimum_required', 0)
                                            calc_method = test.get('calculation_method', 'manual')
                                            
                                            if calc_method == 'increment':
                                                details = f"Rows: {actual_count:,} (minimum: {min_required:,}, method: increment trend)"
                                            else:
                                                details = f"Rows: {actual_count:,} (minimum: {min_required:,}, method: {calc_method})"
                                        
                                        # Create a Paragraph for the details to enable text wrapping
                                        details_para = Paragraph(details, styles['DetailsText'])
                                        
                                        dimension_data.append([
                                            '',  # Empty for dimension grouping
                                            test_name,
                                            Paragraph(f"<font color='{color_name}'><b>{status}</b></font>", styles['Normal']),
                                            details_para
                                        ])
                            
                            # Create and style the dimension results table
                            dimension_table = Table(dimension_data, colWidths=[80, 120, 60, 230])
                            dimension_table.setStyle(TableStyle([
                                # Header styling
                                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                ('FONTSIZE', (0, 0), (-1, 0), 11),
                                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Align content to top of cell
                                
                                # Body styling
                                ('FONTSIZE', (0, 1), (-1, -1), 9,),
                                ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                                ('ALIGN', (2, 1), (2, -1), 'CENTER'),
                                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                                
                                # Grid and padding
                                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7')),
                                ('TOPPADDING', (0, 0), (-1, -1), 8),
                                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                                ('LEFTPADDING', (0, 0), (-1, -1), 6),
                                ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                                
                                # Alternating row colors
                                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
                            ]))
                            
                            story.append(dimension_table)
                            story.append(Spacer(1, 20))
            
            # Build the PDF
            doc.build(story)
            logger.info(f"PDF report generated: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {str(e)}")
            return False
    
    def _add_comparison_section(self, story, results, styles, detailed=False):
        """
        Add a section for dev-prod row count comparisons
        
        Args:
            story: The PDF story to add content to
            results: List of test results
            styles: Document styles
            detailed: If True, include detailed comparison table
        """
        # Create a temporary document to calculate widths if needed
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate
        
        # Use letter size as default for width calculations
        page_width = letter[0] - 60  # Account for margins
        # Extract all comparison tests
        comparison_tests = []
        seen_comparisons = set()  # Track unique database.table combinations
        total_results = len(results)
        total_custom_tests = 0
        
        for result in results:
            custom_tests = result.get('custom_tests', [])
            total_custom_tests += len(custom_tests)
            for test in custom_tests:
                if test.get('test_name') == 'row_count_comparison':
                    # Create unique key to avoid duplicates
                    unique_key = f"{result['database']}.{result['table']}"
                    if unique_key not in seen_comparisons:
                        seen_comparisons.add(unique_key)
                        comparison_tests.append({
                            'database': result['database'],
                            'table': result['table'],
                            'passed': test['passed'],
                            'dev_count': test.get('dev_row_count', 0),
                            'prod_count': test.get('prod_row_count', 0),
                            'difference': test.get('difference', 0),
                            'details': test.get('details', '')
                        })
        
        print(f"PDF Generator Debug: Found {total_results} results, {total_custom_tests} total custom tests, {len(comparison_tests)} unique row count comparisons")
        
        if not comparison_tests:
            print("PDF Generator Debug: No comparison tests found, skipping section")
            return
            
        # Add section header (only if not already added)
        story.append(Paragraph("📊 Row Count Analysis", styles['TableHeader']))
        story.append(Spacer(1, 12))
        
        if not detailed:
            # Group comparison tests by database
            databases = {}
            for test in comparison_tests:
                db_name = test['database']
                if db_name not in databases:
                    databases[db_name] = []
                databases[db_name].append(test)
            
            # Create one table per database
            for db_name, db_tests in databases.items():
                # Add database header
                story.append(Paragraph(f"🗄️ Database: {db_name}", styles['SectionHeader']))
                story.append(Spacer(1, 8))
                
                # Create table data with simplified columns
                table_data = [
                    ['Table Name', 'DEV Count', 'PROD Count', 'Difference', 'Status']
                ]
                
                for test in db_tests:
                    status = '✅ PASS' if test['passed'] else '❌ FAIL'
                    
                    # Format numbers with commas for better readability
                    dev_formatted = f"{test['dev_count']:,}"
                    prod_formatted = f"{test['prod_count']:,}"
                    diff_formatted = f"{test['difference']:,}"
                    
                    # Add percentage if meaningful
                    if test['prod_count'] > 0:
                        percentage = (test['difference'] / test['prod_count']) * 100
                        diff_formatted += f" ({percentage:+.1f}%)"
                    
                    table_data.append([
                        test['table'],
                        dev_formatted,
                        prod_formatted,
                        diff_formatted,
                        Paragraph(f"<font color='{'#4CAF50' if test['passed'] else '#F44336'}'><b>{status}</b></font>", 
                                 styles['Normal'])
                    ])
                
                # Create and style the table with better proportions
                col_widths = [page_width * 0.25, page_width * 0.18, page_width * 0.18, 
                             page_width * 0.19, page_width * 0.2]
                table = Table(table_data, colWidths=col_widths, repeatRows=1)
                table.setStyle(TableStyle([
                    # Header styling with environment theme
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4e79')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('TOPPADDING', (0, 0), (-1, 0), 12),
                    
                    # Body styling
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#2c3e50')),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('ALIGN', (1, 1), (3, -1), 'RIGHT'),  # Right align numbers
                    ('ALIGN', (0, 1), (0, -1), 'LEFT'),   # Left align table names
                    ('ALIGN', (4, 1), (4, -1), 'CENTER'), # Center align status
                    
                    # Grid and visual enhancements
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7')),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')])
                ]))
                
                story.append(table)
                story.append(Spacer(1, 16))  # Space between database tables
            
            # Add interpretation guide
            story.append(Paragraph("📋 Interpretation Guide:", styles['TableHeader']))
            interpretation_text = [
                "• <b>Compliant:</b> DEV row count is less than or equal to PROD (expected behavior)",
                "• <b>Non-Compliant:</b> DEV row count exceeds PROD (requires investigation)",
                "• <b>Variance:</b> Shows absolute difference and percentage change from PROD baseline"
            ]
            for text in interpretation_text:
                story.append(Paragraph(text, styles['Normal']))
                story.append(Spacer(1, 4))
            story.append(Spacer(1, 16))
            
        else:
            # Detailed view - show more information
            for test in comparison_tests:
                # Table header with status indicator
                status_color = colors.green if test['passed'] else colors.red
                status_text = 'PASS' if test['passed'] else 'FAIL'
                
                story.append(Paragraph(
                    f"<b>Table:</b> {test['database']}.{test['table']} ", 
                    styles['TableHeader']
                ))
                
                # Key metrics
                metrics_data = [
                    ['Metric', 'Value'],
                    ['DEV Row Count', f"{test['dev_count']:,}"],
                    ['PROD Row Count', f"{test['prod_count']:,}"],
                    ['Difference', f"{test['difference']:,}"],
                    ['Status', Paragraph(
                        f"<font color='{'#4CAF50' if test['passed'] else '#F44336'}'>{status_text}</font>", 
                        styles['Normal']
                    )]
                ]
                
                metrics_table = Table(metrics_data, colWidths=[page_width * 0.3, page_width * 0.7])
                metrics_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f5f7fa')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#2c3e50')),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e0e0e0')),
                    ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7')),
                ]))
                
                story.append(metrics_table)
                story.append(Spacer(1, 12))
                
                # Details
                story.append(Paragraph("<b>Details:</b> " + test['details'], styles['Normal']))
                story.append(Spacer(1, 24))
    
    def generate_report(self, results: List[Dict[str, Any]], 
                       output_dir: Optional[str] = None, 
                       filename: Optional[str] = None) -> Optional[str]:
        """
        Generate a comprehensive data quality report
        
        Args:
            results (List[Dict[str, Any]]): Test results
            output_dir (str, optional): Output directory
            filename (str, optional): Output filename
            
        Returns:
            str: Path to generated report or None if failed
        """
        try:
            # Use default output directory if not specified
            if output_dir is None:
                output_dir = OUTPUT_CONFIG.get('pdf_output_dir', 'reports')
                
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp if not specified
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data_quality_report_{timestamp}.pdf"
            
            output_path = os.path.join(output_dir, filename)
            
            # Generate the PDF
            success = self._create_pdf_content(results, output_path)
            
            if success:
                logger.info(f"Report generated: {os.path.abspath(output_path)}")
                return output_path
            else:
                logger.error("Failed to generate report")
                return None
                
        except Exception as e:
            logger.error(f"Error in generate_report: {str(e)}")
            return None
