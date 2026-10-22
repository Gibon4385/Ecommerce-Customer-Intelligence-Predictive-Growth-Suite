import json
import os
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Optional imports for Slack/Teams (commented out by default)
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# from airflow.providers.http.operators.http import SimpleHttpOperator

# 1. Define Timezone (Taipei Time UTC+8)
local_tz = pendulum.timezone("Asia/Taipei")

# 2. Define configuration variables
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/ecommerce_dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt/ecommerce_dbt")
DBT_PROJECT = os.environ.get("DBT_PROJECT")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "your-alerts-email@example.com")
DBT_BIN = "/opt/airflow/venv_dbt/bin/dbt"
DBT_PYTHON = "/opt/airflow/venv_dbt/bin/python"
DBT_METRICS_PUBLISHER = os.path.join(
    DBT_PROJECT_DIR, "..", "scripts", "publish_dbt_metrics.py"
)


# 3. Task Failure Callback
def task_failure_alert(context):
    """
    Callback function that runs whenever any task in the DAG fails.
    Sends a customized email alert and provides placeholders for Slack/Teams notifications.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get("ds")
    log_url = task_instance.log_url
    exception = context.get("exception")

    # 1. Send Custom Email Alert (Beautiful HTML Format)
    subject = f"🚨 Airflow Task Failure: {dag_id}.{task_id} ({execution_date})"

    email_html = f"""
    <h3>🚨 Airflow Task Failure Alert</h3>
    <p>A task in your daily DAG has failed and exhausted its retries (or failed directly).</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; min-width: 500px;">
        <tr style="background-color: #f8d7da; color: #721c24;">
            <td><strong>DAG ID</strong></td>
            <td><code>{dag_id}</code></td>
        </tr>
        <tr>
            <td><strong>Task ID</strong></td>
            <td><code>{task_id}</code></td>
        </tr>
        <tr>
            <td><strong>Execution Date</strong></td>
            <td>{execution_date}</td>
        </tr>
        <tr>
            <td><strong>Log URL</strong></td>
            <td><a href="{log_url}" target="_blank" style="color: #007bff; text-decoration: none; font-weight: bold;">View Airflow Logs</a></td>
        </tr>
        <tr>
            <td><strong>Error/Exception</strong></td>
            <td><pre style="white-space: pre-wrap; font-family: monospace; background-color: #f8f9fa; padding: 10px; border: 1px solid #e2e8f0; border-radius: 4px;">{exception}</pre></td>
        </tr>
    </table>
    <p style="color: #7f8c8d; font-size: 11px; margin-top: 15px;">
        This alert was automatically triggered by the task's failure callback.
    </p>
    """

    try:
        send_email(
            to=[ALERT_EMAIL],
            subject=subject,
            html_content=email_html,
        )
        print(f"✅ Failure email alert sent for task {task_id}.")
    except Exception as e:
        print(f"❌ Failed to send failure email alert: {str(e)}")

    # 2. Slack Alert Placeholder
    ENABLE_SLACK = False
    if ENABLE_SLACK:
        # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
        # slack_msg = f"🚨 *Airflow Task Failure Alert*\n*DAG*: `{dag_id}`\n*Task*: `{task_id}`\n*Execution Date*: `{execution_date}`\n*Logs*: <{log_url}|Link>"
        # slack_op = SlackWebhookOperator(
        #     task_id="slack_failure_notifier",
        #     http_conn_id="slack_conn",
        #     message=slack_msg
        # )
        # slack_op.execute(context=context)
        pass

    # 3. Teams Alert Placeholder
    ENABLE_TEAMS = False
    if ENABLE_TEAMS:
        # from airflow.providers.http.operators.http import SimpleHttpOperator
        # teams_payload = {
        #     "@type": "MessageCard",
        #     "@context": "http://schema.org/extensions",
        #     "themeColor": "A20025",
        #     "summary": f"Task Failure: {task_id}",
        #     "sections": [{
        #         "activityTitle": f"🚨 Task Failure in DAG {dag_id}",
        #         "facts": [
        #             {"name": "Task", "value": task_id},
        #             {"name": "Date", "value": execution_date},
        #             {"name": "Log URL", "value": f"[View Logs]({log_url})"}
        #         ],
        #         "markdown": True
        #     }]
        # }
        # teams_op = SimpleHttpOperator(
        #     task_id="teams_failure_notifier",
        #     http_conn_id="teams_conn",
        #     endpoint="",
        #     method="POST",
        #     data=json.dumps(teams_payload),
        #     headers={"Content-Type": "application/json"}
        # )
        # teams_op.execute(context=context)
        pass


# 4. Default Arguments with Retry Policy & Custom Callback
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,  # Disabled standard email to prevent duplicate alerts
    "email": [ALERT_EMAIL],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}


def parse_dbt_results_and_notify(**context):
    """
    Parses the local dbt run_results.json artifact.
    Sends an Email report and prepares commented-out templates for Slack and Teams.
    This method has ZERO query cost on BigQuery and is completely risk-free.
    """
    run_results_path = os.path.join(DBT_PROJECT_DIR, "target", "run_results.json")

    if not os.path.exists(run_results_path):
        print(f"Warning: dbt run_results.json not found at {run_results_path}")
        return

    with open(run_results_path, "r") as f:
        data = json.load(f)

    results = data.get("results", [])
    execution_date = context["ds"]  # YYYY-MM-DD of the run

    # Construct HTML Email Content
    email_html = f"""
    <h3>📊 dbt Daily Execution Report ({execution_date})</h3>
    <p>Below is the summary of the daily dbt run. All metrics are extracted from local artifacts (no BigQuery queries executed).</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; min-width: 500px;">
        <thead style="background-color: #f2f2f2;">
            <tr>
                <th align="left">Model / Task Name</th>
                <th align="left">Type</th>
                <th align="left">Status</th>
                <th align="right">Rows Processed</th>
                <th align="right">Duration (s)</th>
            </tr>
        </thead>
        <tbody>
    """

    slack_text_lines = [f"📢 *dbt Daily Execution Report ({execution_date})*"]
    teams_facts = []

    for r in results:
        unique_id = r.get("unique_id", "")
        # e.g., model.ecommerce_dbt.mart_c360_table -> model, ecommerce_dbt, mart_c360_table
        parts = unique_id.split(".")
        node_type = parts[0] if len(parts) > 0 else "unknown"
        model_name = parts[-1] if len(parts) > 1 else "unknown"

        status = r.get("status", "unknown")
        duration = round(r.get("execution_time", 0.0), 2)

        # Extract adapter response (contains row counts for tables/incremental models in BigQuery)
        adapter_resp = r.get("adapter_response", {})
        rows_affected = adapter_resp.get("rows_affected") if adapter_resp else None

        # NOTE on Rows Processed:
        # - For Views (staging/intermediate): rows_affected is always None / '-' because views are metadata DDLs.
        # - For Tables (marts): rows_affected is the total table size (as they are fully rebuilt).
        # - If a model is changed to 'incremental' in the future, rows_affected will automatically reflect the count of new/yesterday's records.
        if rows_affected is None:
            rows_str = "-"
        else:
            rows_str = f"{rows_affected:,}"

        # Email styling based on status
        status_color = "#2ecc71" if status in ("success", "pass") else "#e74c3c"
        email_html += f"""
        <tr>
            <td><code>{model_name}</code></td>
            <td>{node_type.upper()}</td>
            <td style="color: {status_color}; font-weight: bold;">{status.upper()}</td>
            <td align="right">{rows_str}</td>
            <td align="right">{duration}s</td>
        </tr>
        """

        # Append to Slack / Teams summaries
        slack_text_lines.append(
            f"• *{model_name}* ({node_type}): `{status.upper()}` | Rows: `{rows_str}` | `{duration}s`"
        )
        teams_facts.append(
            {
                "name": model_name,
                "value": f"Status: {status.upper()} | Rows: {rows_str} | Time: {duration}s",
            }
        )

    email_html += """
        </tbody>
    </table>
    <p style="color: #7f8c8d; font-size: 11px; margin-top: 15px;">
        *Note: Views do not show row counts. Tables show total records. Incremental models show newly appended records.
    </p>
    """

    # ------------------ 1. SEND EMAIL (Active) ------------------
    send_email(
        to=[ALERT_EMAIL],
        subject=f"dbt Execution Summary - {execution_date}",
        html_content=email_html,
    )
    print("✅ Email notification sent successfully.")

    # ------------------ 2. SEND SLACK (Example / Disabled) ------------------
    # To enable Slack, set ENABLE_SLACK = True and configure a Slack webhook Connection in Airflow.
    ENABLE_SLACK = False
    if ENABLE_SLACK:
        # slack_payload = {
        #     "text": "\n".join(slack_text_lines)
        # }
        # slack_op = SlackWebhookOperator(
        #     task_id="slack_notifier",
        #     http_conn_id="slack_conn", # Set this in Airflow Connections
        #     message=slack_payload["text"]
        # )
        # slack_op.execute(context=context)
        print("ℹ️ Slack notification template is ready but disabled.")
        pass

    # ------------------ 3. SEND TEAMS (Example / Disabled) ------------------
    # To enable Teams, set ENABLE_TEAMS = True and configure a Http Webhook Connection in Airflow.
    ENABLE_TEAMS = False
    if ENABLE_TEAMS:
        # teams_payload = {
        #     "@type": "MessageCard",
        #     "@context": "http://schema.org/extensions",
        #     "themeColor": "0076D7",
        #     "summary": f"dbt Daily Execution - {execution_date}",
        #     "sections": [{
        #         "activityTitle": f"dbt Daily Execution Report - {execution_date}",
        #         "facts": teams_facts,
        #         "markdown": True
        #     }]
        # }
        # teams_op = SimpleHttpOperator(
        #     task_id="teams_notifier",
        #     http_conn_id="teams_conn", # Set this in Airflow Connections
        #     endpoint="",
        #     method="POST",
        #     data=json.dumps(teams_payload),
        #     headers={"Content-Type": "application/json"}
        # )
        # teams_op.execute(context=context)
        print("ℹ️ Teams notification template is ready but disabled.")
        pass


# 4. Define DAG
with DAG(
    dag_id="ecommerce_dbt_daily_run",
    default_args=default_args,
    description="Daily execution of dbt transformation (Raw to Business Models) at 1:00 AM local time",
    schedule="0 1 * * *",  # Run daily at 01:00 AM (Taipei Time)
    start_date=datetime(2026, 7, 1, tzinfo=local_tz),
    catchup=False,
    tags=["dbt", "ecommerce", "production"],
) as dag:
    # Task 1: Check dbt connectivity and run debug
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} debug --target prod",
        env={
            "DBT_PROJECT": DBT_PROJECT,
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            **os.environ,
        },
    )

    # Task 2: Install dbt dependencies (useful if using packages.yml)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps",
        env={
            "DBT_PROJECT": DBT_PROJECT,
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            **os.environ,
        },
    )

    # Task 3: Build models (run & test & snapshot)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --target prod",
        env={
            "DBT_PROJECT": DBT_PROJECT,
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            **os.environ,
        },
    )

    # Task 4: Evaluate freshness for streaming sources. The separate target
    # path preserves dbt_build's run_results.json for model/test metrics.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && {DBT_BIN} source freshness --target prod "
            "--target-path target/freshness"
        ),
        env={
            "DBT_PROJECT": DBT_PROJECT,
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            **os.environ,
        },
    )

    # Task 5: Publish run_results.json and sources.json as custom metrics.
    publish_monitoring_metrics = BashOperator(
        task_id="publish_cloud_monitoring_metrics",
        trigger_rule="all_done",
        bash_command=(
            f"{DBT_PYTHON} {DBT_METRICS_PUBLISHER} "
            f"--project-id {DBT_PROJECT} "
            f"--run-results {DBT_PROJECT_DIR}/target/run_results.json "
            f"--sources {DBT_PROJECT_DIR}/target/freshness/sources.json"
        ),
    )

    # Task 6: Parse results and send daily report notifications
    # This runs AFTER dbt_build completes successfully
    send_report = PythonOperator(
        task_id="send_daily_report",
        python_callable=parse_dbt_results_and_notify,
    )

    # Define task dependencies
    (
        dbt_debug
        >> dbt_deps
        >> dbt_build
        >> dbt_source_freshness
        >> publish_monitoring_metrics
        >> send_report
    )
