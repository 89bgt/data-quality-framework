"""
Data Quality Framework - Airflow DAG
Runs data quality checks automatically every day at 6 AM
Can also be triggered manually from Airflow UI
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']  
}

# DAG Definition
dag = DAG(
    'data_quality_framework',
    default_args=default_args,
    description='Daily Data Quality Monitoring Framework',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one instance at a time
    tags=['data-quality', 'monitoring', 'daily']
)

# Configuration Variables (set these in Airflow Variables)
# Go to Admin -> Variables in Airflow UI to set these:
PROJECT_PATH = Variable.get("dq_project_path", "/path/to/data_quality_framework")
PYTHON_ENV_PATH = Variable.get("dq_python_env", "/path/to/final_env")
LOG_RETENTION_DAYS = Variable.get("dq_log_retention_days", "30")

def check_environment():
    """
    Check if the environment and project path exist
    """
    if not os.path.exists(PROJECT_PATH):
        raise FileNotFoundError(f"Project path does not exist: {PROJECT_PATH}")
    
    if not os.path.exists(PYTHON_ENV_PATH):
        raise FileNotFoundError(f"Python environment does not exist: {PYTHON_ENV_PATH}")
    
    main_py_path = os.path.join(PROJECT_PATH, "main.py")
    if not os.path.exists(main_py_path):
        raise FileNotFoundError(f"main.py not found in project path: {main_py_path}")
    
    logger.info(f"Environment check passed. Project: {PROJECT_PATH}, Python: {PYTHON_ENV_PATH}")
    return True

def cleanup_old_logs():
    """
    Clean up log files older than specified retention period
    """
    try:
        import glob
        from datetime import datetime, timedelta
        
        log_pattern = os.path.join(PROJECT_PATH, "data_quality_log_*.txt")
        log_files = glob.glob(log_pattern)
        
        cutoff_date = datetime.now() - timedelta(days=int(LOG_RETENTION_DAYS))
        cleaned_count = 0
        
        for log_file in log_files:
            file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
            if file_time < cutoff_date:
                os.remove(log_file)
                cleaned_count += 1
                logger.info(f"Removed old log file: {log_file}")
        
        logger.info(f"Cleaned up {cleaned_count} old log files")
        return cleaned_count
        
    except Exception as e:
        logger.warning(f"Failed to cleanup old logs: {str(e)}")
        return 0

def send_failure_notification(context):
    """
    Send notification when DAG fails
    """
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    subject = f"❌ Data Quality Framework Failed - {dag_run.execution_date.strftime('%Y-%m-%d')}"
    
    html_content = f"""
    <h3>Data Quality Framework Execution Failed</h3>
    <p><strong>DAG:</strong> {dag_run.dag_id}</p>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
    <p><strong>Log URL:</strong> <a href="{task_instance.log_url}">View Logs</a></p>
    
    <h4>Troubleshooting Steps:</h4>
    <ul>
        <li>Check the Airflow logs for detailed error messages</li>
        <li>Verify database connections (Hive and PostgreSQL)</li>
        <li>Ensure the Python environment is accessible</li>
        <li>Check if the project path exists and is readable</li>
    </ul>
    
    <p>Please investigate and resolve the issue.</p>
    """
    
    return EmailOperator(
        task_id='send_failure_email',
        to=['data-team@company.com'],  # Change this
        subject=subject,
        html_content=html_content,
        dag=dag
    )

# Task 1: Environment Check
check_env_task = PythonOperator(
    task_id='check_environment',
    python_callable=check_environment,
    dag=dag,
    doc_md="""
    ## Environment Check Task
    
    This task verifies that:
    - Project directory exists and is accessible
    - Python virtual environment exists
    - main.py file exists in the project directory
    
    If any of these checks fail, the DAG will stop execution.
    """
)

# Task 2: Run Data Quality Framework
run_dq_framework = BashOperator(
    task_id='run_data_quality_checks',
    bash_command=f"""
    set -e
    
    # Navigate to project directory
    cd {PROJECT_PATH}
    
    # Activate virtual environment and run framework with logging
    source {PYTHON_ENV_PATH}/bin/activate || source {PYTHON_ENV_PATH}/Scripts/activate
    
    # Create timestamped log file
    LOG_FILE="data_quality_log_$(date +%Y%m%d_%H%M%S).txt"
    
    # Run the framework with comprehensive logging
    echo "Starting Data Quality Framework at $(date)"
    echo "Project Path: {PROJECT_PATH}"
    echo "Python Environment: {PYTHON_ENV_PATH}"
    echo "Log File: $LOG_FILE"
    echo "----------------------------------------"
    
    # Execute the framework and capture all output
    python main.py 2>&1 | tee "$LOG_FILE"
    
    # Check exit status
    EXIT_CODE=${{PIPESTATUS[0]}}
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Data Quality Framework completed successfully"
        echo "📄 Log file: $LOG_FILE"
        echo "📊 Check the reports/ directory for PDF reports"
        echo "📋 Check the results/ directory for JSON results"
    else
        echo "❌ Data Quality Framework failed with exit code: $EXIT_CODE"
        echo "📄 Check log file for details: $LOG_FILE"
        exit $EXIT_CODE
    fi
    """,
    dag=dag,
    on_failure_callback=send_failure_notification,
    doc_md="""
    ## Data Quality Framework Execution
    
    This task:
    1. Activates the Python virtual environment
    2. Runs the data quality framework
    3. Captures all output to a timestamped log file
    4. Generates PDF and JSON reports
    5. Stores results in PostgreSQL (if configured)
    6. Sends email notifications (if configured)
    
    The task will fail if:
    - Database connections fail
    - No valid tables are found
    - Critical errors occur during execution
    
    Check the generated log file for detailed information.
    """
)

# Task 3: Cleanup Old Logs
cleanup_logs_task = PythonOperator(
    task_id='cleanup_old_logs',
    python_callable=cleanup_old_logs,
    dag=dag,
    trigger_rule='none_failed',  # Run even if previous task fails
    doc_md=f"""
    ## Log Cleanup Task
    
    This task removes log files older than {LOG_RETENTION_DAYS} days to prevent disk space issues.
    
    Files matching pattern: `data_quality_log_*.txt`
    
    This task runs regardless of whether the main framework task succeeds or fails.
    """
)

# Task 4: Success Notification (Optional)
success_notification = BashOperator(
    task_id='send_success_notification',
    bash_command=f"""
    echo "✅ Data Quality Framework completed successfully at $(date)"
    echo "📊 Reports generated in: {PROJECT_PATH}/reports/"
    echo "📋 Results saved in: {PROJECT_PATH}/results/"
    echo "📄 Latest log file: $(ls -t {PROJECT_PATH}/data_quality_log_*.txt | head -1)"
    
    # Optional: Send success notification to Slack/Teams/etc.
    # curl -X POST -H 'Content-type: application/json' --data '{{"text":"✅ Data Quality checks completed successfully"}}' YOUR_WEBHOOK_URL
    """,
    dag=dag,
    doc_md="""
    ## Success Notification
    
    This task runs only when the data quality framework completes successfully.
    
    You can customize this task to:
    - Send notifications to Slack/Teams
    - Update monitoring dashboards
    - Trigger downstream processes
    """
)

# Define task dependencies
check_env_task >> run_dq_framework >> [cleanup_logs_task, success_notification]

# DAG Documentation
dag.doc_md = """
# Data Quality Framework - Airflow DAG

## Overview
This DAG runs the Data Quality Framework automatically every day at 6 AM and can also be triggered manually.

## Schedule
- **Automatic**: Daily at 6:00 AM
- **Manual**: Can be triggered anytime from Airflow UI

## What it does
1. **Environment Check**: Verifies project path and Python environment
2. **Data Quality Execution**: Runs the framework with comprehensive logging
3. **Log Cleanup**: Removes old log files to save disk space
4. **Notifications**: Sends success/failure notifications

## Configuration Required

### Airflow Variables (Admin -> Variables)
Set these variables in Airflow UI:

| Variable | Description | Example |
|----------|-------------|---------|
| `dq_project_path` | Full path to data quality framework | `/home/user/data_quality_framework` |
| `dq_python_env` | Full path to Python virtual environment | `/home/user/final_env` |
| `dq_log_retention_days` | Days to keep log files | `30` |

### Email Configuration
Update the email addresses in the DAG:
- `default_args['email']` - For failure notifications
- `send_failure_notification()` function - For detailed failure emails

## Manual Execution
To run manually:
1. Go to Airflow UI
2. Find "data_quality_framework" DAG
3. Click the "Trigger DAG" button
4. Optionally add configuration parameters

## Monitoring
- Check Airflow logs for execution details
- Review generated log files in project directory
- Monitor PDF/JSON reports in output directories
- Check PostgreSQL tables for historical data

## Troubleshooting
If the DAG fails:
1. Check Airflow task logs
2. Verify Airflow Variables are set correctly
3. Ensure database connections work
4. Check file permissions on project directory
5. Verify Python environment is accessible

## Files Generated
- **Log Files**: `data_quality_log_YYYYMMDD_HHMMSS.txt`
- **PDF Reports**: `reports/data_quality_report_YYYYMMDD_HHMMSS.pdf`
- **JSON Results**: `results/data_quality_results_YYYYMMDD_HHMMSS.json`
"""
