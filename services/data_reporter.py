"""
Data quality reporting service for generating comprehensive reports and visualizations.
"""

import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO

from models.data_models import DataValidationResult, CustomerTransaction, DataIngestionStats, DataQualityLevel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityReporter:
    """Service for generating data quality reports and visualizations"""
    
    def __init__(self):
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)
        
        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")
    
    def generate_comprehensive_report(self, 
                                    validation_results: List[DataValidationResult],
                                    stats: DataIngestionStats,
                                    output_format: str = "html") -> str:
        """
        Generate a comprehensive data quality report
        
        Args:
            validation_results: List of validation results
            stats: Processing statistics
            output_format: Output format (html, markdown, txt)
            
        Returns:
            str: Path to generated report file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"data_quality_report_{timestamp}.{output_format}"
        report_path = self.reports_dir / report_filename
        
        logger.info(f"Generating comprehensive report: {report_path}")
        
        # Prepare data for analysis
        good_data = [r for r in validation_results if r.is_valid]
        bad_data = [r for r in validation_results if not r.is_valid]
        
        # Generate report content
        if output_format == "html":
            content = self._generate_html_report(validation_results, stats, good_data, bad_data)
        elif output_format == "markdown":
            content = self._generate_markdown_report(validation_results, stats, good_data, bad_data)
        else:
            content = self._generate_text_report(validation_results, stats, good_data, bad_data)
        
        # Write report to file
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Report generated successfully: {report_path}")
        return str(report_path)
    
    def _generate_html_report(self, validation_results, stats, good_data, bad_data) -> str:
        """Generate HTML report"""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; }}
        .metric {{ display: inline-block; margin: 10px; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .error {{ background-color: #ffe6e6; }}
        .success {{ background-color: #e6ffe6; }}
        .warning {{ background-color: #fff3e6; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report</h1>
        <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="section">
        <h2>Executive Summary</h2>
        {self._generate_summary_metrics_html(stats)}
    </div>
    
    <div class="section">
        <h2>Quality Distribution</h2>
        {self._generate_quality_distribution_html(stats)}
    </div>
    
    <div class="section">
        <h2>Validation Details</h2>
        {self._generate_validation_details_html(validation_results)}
    </div>
    
    <div class="section">
        <h2>Error Analysis</h2>
        {self._generate_error_analysis_html(bad_data)}
    </div>
    
    <div class="section">
        <h2>Data Quality Insights</h2>
        {self._generate_insights_html(validation_results, stats)}
    </div>
</body>
</html>
        """
        return html_content
    
    def _generate_markdown_report(self, validation_results, stats, good_data, bad_data) -> str:
        """Generate Markdown report"""
        content = f"""# Data Quality Report

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary

{self._generate_summary_metrics_markdown(stats)}

## Quality Distribution

{self._generate_quality_distribution_markdown(stats)}

## Validation Details

- Total Records: {stats.total_records:,}
- Valid Records: {stats.valid_records:,}
- Invalid Records: {stats.invalid_records:,}
- Success Rate: {stats.success_rate:.2f}%

## Error Analysis

{self._generate_error_analysis_markdown(bad_data)}

## Data Quality Insights

{self._generate_insights_markdown(validation_results, stats)}
"""
        return content
    
    def _generate_text_report(self, validation_results, stats, good_data, bad_data) -> str:
        """Generate plain text report"""
        content = f"""
DATA QUALITY REPORT
==================

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

EXECUTIVE SUMMARY
-----------------
Total Records: {stats.total_records:,}
Valid Records: {stats.valid_records:,}
Invalid Records: {stats.invalid_records:,}
Success Rate: {stats.success_rate:.2f}%
Processing Duration: {stats.processing_duration:.2f}s

QUALITY DISTRIBUTION
--------------------
Excellent Quality: {stats.excellent_quality:,}
Good Quality: {stats.good_quality:,}
Fair Quality: {stats.fair_quality:,}
Poor Quality: {stats.poor_quality:,}

ERROR ANALYSIS
--------------
{self._generate_error_analysis_text(bad_data)}

DATA QUALITY INSIGHTS
---------------------
{self._generate_insights_text(validation_results, stats)}
"""
        return content
    
    def _generate_summary_metrics_html(self, stats: DataIngestionStats) -> str:
        """Generate HTML summary metrics"""
        success_class = "success" if stats.success_rate >= 90 else "warning" if stats.success_rate >= 70 else "error"
        
        return f"""
        <div class="metric success">
            <h3>Total Records</h3>
            <p>{stats.total_records:,}</p>
        </div>
        <div class="metric {success_class}">
            <h3>Success Rate</h3>
            <p>{stats.success_rate:.2f}%</p>
        </div>
        <div class="metric">
            <h3>Processing Time</h3>
            <p>{stats.processing_duration:.2f}s</p>
        </div>
        <div class="metric">
            <h3>Records/Second</h3>
            <p>{stats.total_records / stats.processing_duration if stats.processing_duration else 0:.2f}</p>
        </div>
        """
    
    def _generate_quality_distribution_html(self, stats: DataIngestionStats) -> str:
        """Generate HTML quality distribution"""
        total_valid = stats.valid_records
        if total_valid == 0:
            return "<p>No valid records to analyze.</p>"
        
        return f"""
        <table>
            <tr>
                <th>Quality Level</th>
                <th>Count</th>
                <th>Percentage</th>
            </tr>
            <tr>
                <td>Excellent</td>
                <td>{stats.excellent_quality:,}</td>
                <td>{(stats.excellent_quality / total_valid * 100):.1f}%</td>
            </tr>
            <tr>
                <td>Good</td>
                <td>{stats.good_quality:,}</td>
                <td>{(stats.good_quality / total_valid * 100):.1f}%</td>
            </tr>
            <tr>
                <td>Fair</td>
                <td>{stats.fair_quality:,}</td>
                <td>{(stats.fair_quality / total_valid * 100):.1f}%</td>
            </tr>
            <tr>
                <td>Poor</td>
                <td>{stats.poor_quality:,}</td>
                <td>{(stats.poor_quality / total_valid * 100):.1f}%</td>
            </tr>
        </table>
        """
    
    def _generate_validation_details_html(self, validation_results: List[DataValidationResult]) -> str:
        """Generate HTML validation details"""
        if not validation_results:
            return "<p>No validation results to display.</p>"
        
        # Sample of validation results
        sample_size = min(10, len(validation_results))
        sample_results = validation_results[:sample_size]
        
        html = "<table><tr><th>Status</th><th>Quality Score</th><th>Errors</th><th>Warnings</th></tr>"
        
        for result in sample_results:
            status = "✅ Valid" if result.is_valid else "❌ Invalid"
            quality_score = f"{result.quality_score:.2f}" if result.quality_score else "N/A"
            errors = len(result.errors)
            warnings = len(result.warnings)
            
            html += f"<tr><td>{status}</td><td>{quality_score}</td><td>{errors}</td><td>{warnings}</td></tr>"
        
        html += "</table>"
        
        if len(validation_results) > sample_size:
            html += f"<p><em>Showing first {sample_size} of {len(validation_results)} results</em></p>"
        
        return html
    
    def _generate_error_analysis_html(self, bad_data: List[DataValidationResult]) -> str:
        """Generate HTML error analysis"""
        if not bad_data:
            return "<p>No validation errors found! 🎉</p>"
        
        # Analyze error patterns
        error_patterns = {}
        for result in bad_data:
            for error in result.errors:
                field = error.split(':')[0] if ':' in error else 'general'
                error_patterns[field] = error_patterns.get(field, 0) + 1
        
        html = "<h3>Common Error Patterns</h3><table><tr><th>Error Type</th><th>Count</th></tr>"
        
        for error_type, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            html += f"<tr><td>{error_type}</td><td>{count}</td></tr>"
        
        html += "</table>"
        return html
    
    def _generate_summary_metrics_markdown(self, stats: DataIngestionStats) -> str:
        """Generate Markdown summary metrics"""
        return f"""
| Metric | Value |
|--------|-------|
| Total Records | {stats.total_records:,} |
| Valid Records | {stats.valid_records:,} |
| Invalid Records | {stats.invalid_records:,} |
| Success Rate | {stats.success_rate:.2f}% |
| Processing Duration | {stats.processing_duration:.2f}s |
"""
    
    def _generate_quality_distribution_markdown(self, stats: DataIngestionStats) -> str:
        """Generate Markdown quality distribution"""
        total_valid = stats.valid_records
        if total_valid == 0:
            return "No valid records to analyze."
        
        return f"""
| Quality Level | Count | Percentage |
|---------------|-------|------------|
| Excellent | {stats.excellent_quality:,} | {(stats.excellent_quality / total_valid * 100):.1f}% |
| Good | {stats.good_quality:,} | {(stats.good_quality / total_valid * 100):.1f}% |
| Fair | {stats.fair_quality:,} | {(stats.fair_quality / total_valid * 100):.1f}% |
| Poor | {stats.poor_quality:,} | {(stats.poor_quality / total_valid * 100):.1f}% |
"""
    
    def _generate_error_analysis_markdown(self, bad_data: List[DataValidationResult]) -> str:
        """Generate Markdown error analysis"""
        if not bad_data:
            return "No validation errors found! 🎉"
        
        error_patterns = {}
        for result in bad_data:
            for error in result.errors:
                field = error.split(':')[0] if ':' in error else 'general'
                error_patterns[field] = error_patterns.get(field, 0) + 1
        
        content = "### Common Error Patterns\n\n"
        for error_type, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            content += f"- **{error_type}**: {count} occurrences\n"
        
        return content
    
    def _generate_insights_html(self, validation_results, stats) -> str:
        """Generate HTML insights"""
        insights = self._analyze_data_patterns(validation_results, stats)
        
        html = "<ul>"
        for insight in insights:
            html += f"<li>{insight}</li>"
        html += "</ul>"
        
        return html
    
    def _generate_insights_markdown(self, validation_results, stats) -> str:
        """Generate Markdown insights"""
        insights = self._analyze_data_patterns(validation_results, stats)
        
        content = ""
        for insight in insights:
            content += f"- {insight}\n"
        
        return content
    
    def _generate_insights_text(self, validation_results, stats) -> str:
        """Generate text insights"""
        insights = self._analyze_data_patterns(validation_results, stats)
        
        content = ""
        for i, insight in enumerate(insights, 1):
            content += f"{i}. {insight}\n"
        
        return content
    
    def _generate_error_analysis_text(self, bad_data: List[DataValidationResult]) -> str:
        """Generate text error analysis"""
        if not bad_data:
            return "No validation errors found!"
        
        error_patterns = {}
        for result in bad_data:
            for error in result.errors:
                field = error.split(':')[0] if ':' in error else 'general'
                error_patterns[field] = error_patterns.get(field, 0) + 1
        
        content = "Common Error Patterns:\n"
        for error_type, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            content += f"  - {error_type}: {count} occurrences\n"
        
        return content
    
    def _analyze_data_patterns(self, validation_results: List[DataValidationResult], stats: DataIngestionStats) -> List[str]:
        """Analyze data patterns and generate insights"""
        insights = []
        
        if stats.total_records == 0:
            return ["No data to analyze"]
        
        # Success rate analysis
        if stats.success_rate >= 95:
            insights.append("Excellent data quality with very high success rate")
        elif stats.success_rate >= 85:
            insights.append("Good data quality with acceptable success rate")
        elif stats.success_rate >= 70:
            insights.append("Fair data quality - consider data source review")
        else:
            insights.append("Poor data quality - immediate attention required")
        
        # Quality distribution analysis
        total_valid = stats.valid_records
        if total_valid > 0:
            excellent_pct = (stats.excellent_quality / total_valid) * 100
            poor_pct = (stats.poor_quality / total_valid) * 100
            
            if excellent_pct >= 80:
                insights.append("Majority of valid records have excellent quality scores")
            elif poor_pct >= 30:
                insights.append("High percentage of poor quality records - review data collection processes")
            
            if excellent_pct + stats.good_quality / total_valid * 100 >= 90:
                insights.append("Strong overall data quality suitable for production use")
        
        # Performance analysis
        if stats.processing_duration:
            rps = stats.total_records / stats.processing_duration
            if rps >= 1000:
                insights.append("High processing throughput achieved")
            elif rps < 100:
                insights.append("Low processing throughput - consider optimization")
        
        # Error pattern analysis
        bad_data = [r for r in validation_results if not r.is_valid]
        if bad_data:
            error_patterns = {}
            for result in bad_data:
                for error in result.errors:
                    field = error.split(':')[0] if ':' in error else 'general'
                    error_patterns[field] = error_patterns.get(field, 0) + 1
            
            if error_patterns:
                most_common_error = max(error_patterns, key=error_patterns.get)
                insights.append(f"Most common validation issue: {most_common_error}")
                
                if error_patterns[most_common_error] / len(bad_data) > 0.5:
                    insights.append(f"Over 50% of errors are related to {most_common_error} - focus on fixing this issue")
        
        return insights
    
    def generate_quality_trend_report(self, 
                                    historical_stats: List[DataIngestionStats],
                                    output_path: str = None) -> str:
        """Generate a trend report from historical statistics"""
        if not historical_stats:
            raise ValueError("No historical statistics provided")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = output_path or str(self.reports_dir / f"quality_trend_report_{timestamp}.html")
        
        # Create trend data
        dates = [stat.processing_start_time for stat in historical_stats]
        success_rates = [stat.success_rate for stat in historical_stats]
        total_records = [stat.total_records for stat in historical_stats]
        
        # Generate HTML report with trend visualizations
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Trend Report</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .chart {{ margin: 20px 0; height: 400px; }}
            </style>
        </head>
        <body>
            <h1>Data Quality Trend Report</h1>
            <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div id="success-rate-chart" class="chart"></div>
            <div id="volume-chart" class="chart"></div>
            
            <script>
                // Success rate trend
                var successRateData = [{{
                    x: {[d.isoformat() for d in dates]},
                    y: {success_rates},
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: 'Success Rate %'
                }}];
                
                Plotly.newPlot('success-rate-chart', successRateData, {{
                    title: 'Success Rate Trend',
                    xaxis: {{ title: 'Date' }},
                    yaxis: {{ title: 'Success Rate (%)' }}
                }});
                
                // Volume trend
                var volumeData = [{{
                    x: {[d.isoformat() for d in dates]},
                    y: {total_records},
                    type: 'bar',
                    name: 'Total Records'
                }}];
                
                Plotly.newPlot('volume-chart', volumeData, {{
                    title: 'Data Volume Trend',
                    xaxis: {{ title: 'Date' }},
                    yaxis: {{ title: 'Records Processed' }}
                }});
            </script>
        </body>
        </html>
        """
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Trend report generated: {report_path}")
        return report_path
