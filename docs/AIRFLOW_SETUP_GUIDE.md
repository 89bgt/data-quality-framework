# 🚁 Airflow Setup Guide for Data Quality Framework

## 📋 Prerequisites

- **Apache Airflow** installed and running
- **Data Quality Framework** already configured (see USER_SETUP_GUIDE.md)
- **Access to Airflow UI** (usually http://localhost:8080)

## 🔧 Setup Steps

### Step 1: Copy the DAG File

1. Copy `airflow_dag.py` to your Airflow DAGs folder:
   ```bash
   cp airflow_dag.py /path/to/airflow/dags/
   ```

2. The DAG should appear in Airflow UI within a few minutes

### Step 2: Configure Airflow Variables

Go to **Airflow UI → Admin → Variables** and create these variables:

| Variable Name | Value | Description |
|---------------|-------|-------------|
| `dq_project_path` | `/full/path/to/data_quality_framework` | Full path to your project folder |
| `dq_python_env` | `/full/path/to/final_env` | Full path to your Python virtual environment |
| `dq_log_retention_days` | `30` | How many days to keep log files |

**Example Values:**
- `dq_project_path`: `/path/to/data_quality_framework`
- `dq_python_env`: `/path/to/venv/bin/python`

### Step 3: Update Email Addresses

Edit `airflow_dag.py` and change these email addresses:

**Line 25:**
```python
'email': ['your-team@company.com']  # Change to your team's email
```

**Line 80:**
```python
to=['your-team@company.com'],  # Change to your team's email
```

### Step 4: Test the DAG

1. **Enable the DAG**: In Airflow UI, toggle the DAG to "ON"
2. **Manual Test**: Click "Trigger DAG" to run it manually first
3. **Check Logs**: Monitor the task logs to ensure everything works
4. **Verify Output**: Check that reports are generated in your project folder

## 📅 Schedule Configuration

### Current Schedule
- **Daily at 6:00 AM**: `'0 6 * * *'`

### Change Schedule
Edit line 38 in `airflow_dag.py`:

```python
# Examples:
schedule_interval='0 6 * * *',    # Daily at 6 AM
schedule_interval='0 */6 * * *',  # Every 6 hours
schedule_interval='0 8 * * 1',    # Weekly on Monday at 8 AM
schedule_interval='0 9 1 * *',    # Monthly on 1st at 9 AM
schedule_interval=None,           # Manual trigger only
```

## 🔍 Monitoring

### Airflow UI
- **DAG View**: See task status and dependencies
- **Graph View**: Visual representation of workflow
- **Logs**: Detailed execution logs for each task
- **Gantt Chart**: Task execution timeline

### Generated Files
- **Log Files**: `data_quality_log_YYYYMMDD_HHMMSS.txt`
- **PDF Reports**: `reports/data_quality_report_YYYYMMDD_HHMMSS.pdf`
- **JSON Results**: `results/data_quality_results_YYYYMMDD_HHMMSS.json`

## 🚨 Troubleshooting

### Common Issues

1. **DAG Not Appearing**
   - Check DAG file syntax: `python airflow_dag.py`
   - Verify file is in correct DAGs folder
   - Check Airflow logs for parsing errors

2. **Environment Variables Not Found**
   - Verify Variables are set in Airflow UI
   - Check variable names match exactly
   - Restart Airflow webserver if needed

3. **Permission Errors**
   - Ensure Airflow user can access project directory
   - Check Python environment permissions
   - Verify database connection permissions

4. **Python Environment Issues**
   - Test environment activation manually
   - Verify all required packages are installed
   - Check virtual environment path is correct

### Manual Testing

Test components individually:

```bash
# Test environment activation
source /path/to/final_env/bin/activate  # Linux/Mac
# OR
source /path/to/final_env/Scripts/activate  # Windows

# Test framework execution
cd /path/to/data_quality_framework
python main.py

# Test with logging
python main.py 2>&1 | tee test_log.txt
```

## 📧 Email Notifications

### Setup Email in Airflow

1. **Configure SMTP** in `airflow.cfg`:
   ```ini
   [smtp]
   smtp_host = smtp.gmail.com
   smtp_starttls = True
   smtp_ssl = False
   smtp_user = your-email@gmail.com
   smtp_password = your-app-password
   smtp_port = 587
   smtp_mail_from = your-email@gmail.com
   ```

2. **Restart Airflow** after configuration changes

### Email Types
- **Failure Notifications**: Sent automatically when tasks fail
- **Success Notifications**: Optional, can be customized
- **Detailed Reports**: Include links to logs and troubleshooting steps

## 🎛️ Advanced Configuration

### Custom Parameters
Run with custom parameters by adding to DAG trigger:

```json
{
  "databases": ["preprocessed_dev"],
  "tables": ["specific_table"],
  "skip_email": true
}
```

### Parallel Execution
For multiple databases, modify the DAG to run in parallel:

```python
# Create separate tasks for each database
for db in ['db1', 'db2', 'db3']:
    task = BashOperator(
        task_id=f'run_dq_{db}',
        bash_command=f'python main.py --database {db}',
        dag=dag
    )
```

### Integration with Other Systems
- **Slack Notifications**: Add Slack webhook calls
- **Monitoring Dashboards**: Send metrics to Grafana/Prometheus
- **Data Catalogs**: Update metadata in data catalog systems

## ✅ Quick Checklist

- [ ] Copy `airflow_dag.py` to Airflow DAGs folder
- [ ] Set Airflow Variables (project path, Python env, retention days)
- [ ] Update email addresses in DAG file
- [ ] Configure SMTP settings in Airflow
- [ ] Enable DAG in Airflow UI
- [ ] Test manual trigger
- [ ] Verify reports are generated
- [ ] Check email notifications work
- [ ] Monitor first scheduled run

## 🎯 You're Ready!

Your Data Quality Framework will now run automatically every day and can be triggered manually anytime. The system will:

- ✅ Run comprehensive data quality checks
- 📊 Generate timestamped reports
- 📧 Send notifications on success/failure
- 🧹 Clean up old log files automatically
- 📈 Store historical data for trend analysis

Monitor the Airflow UI regularly to ensure smooth operation!
