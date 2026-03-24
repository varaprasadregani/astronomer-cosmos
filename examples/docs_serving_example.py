"""
Example: Setting up dbt docs with S3 storage
=============================================

This example shows how to configure dbt docs generation and serving from S3.

The key insight: The webserver gets credentials the SAME WAY tasks do -
through Airflow's connection system. No special setup needed!
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from cosmos import DbtTaskGroup, ProfileConfig
from cosmos.operators import DbtDocsS3Operator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# ============================================================================
# STEP 1: Configure dbt profile (for running dbt commands)
# ============================================================================

profile_config = ProfileConfig(
    profile_name="my_dbt_profile",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "ANALYTICS",
            "schema": "DBT_PROD",
        },
    ),
)

# ============================================================================
# STEP 2: Create DAG that generates and uploads docs to S3
# ============================================================================

with DAG(
    dag_id="dbt_docs_with_s3_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
) as dag:
    # Run dbt models
    transform = DbtTaskGroup(
        group_id="transform",
        project_config={"dbt_project_path": "/usr/local/airflow/dags/dbt/my_project"},
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    # Generate and upload dbt docs to S3
    generate_docs = DbtDocsS3Operator(
        task_id="generate_dbt_docs",
        project_dir="/usr/local/airflow/dags/dbt/my_project",
        profile_config=profile_config,
        # S3 configuration - uses Airflow connection
        connection_id="aws_default",  # ← Airflow connection ID
        bucket_name="my-company-dbt-docs",
        folder_dir="production",  # Optional: organize by environment
    )

    # Optional: Verify docs were uploaded successfully
    def verify_docs_uploaded(**context):
        """Verify the docs files exist in S3."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")
        required_files = ["index.html", "manifest.json", "catalog.json"]

        for filename in required_files:
            key = f"production/{filename}"
            if not hook.check_for_key(key=key, bucket_name="my-company-dbt-docs"):
                raise FileNotFoundError(f"Missing required file: {filename}")

        print("✅ All dbt docs files uploaded successfully!")

    verify = PythonOperator(
        task_id="verify_docs_uploaded",
        python_callable=verify_docs_uploaded,
    )

    # Set task dependencies
    transform >> generate_docs >> verify


# ============================================================================
# STEP 3: Configure the webserver to serve these docs
# ============================================================================

"""
Add to your airflow.cfg or set as environment variables:

For Airflow 2:
--------------
[cosmos]
dbt_docs_dir = s3://my-company-dbt-docs/production
dbt_docs_conn_id = aws_default

Or as environment variables:
export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-company-dbt-docs/production"
export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"

For Airflow 3:
--------------
[cosmos]
dbt_docs_projects = {
    "prod": {
        "dir": "s3://my-company-dbt-docs/production",
        "conn_id": "aws_default",
        "name": "dbt Docs (Production)"
    }
}

Or as environment variable:
export AIRFLOW__COSMOS__DBT_DOCS_PROJECTS='{"prod":{"dir":"s3://my-company-dbt-docs/production","conn_id":"aws_default","name":"dbt Docs (Production)"}}'

How the webserver gets credentials:
-----------------------------------
The webserver uses the SAME Airflow connection system as tasks:

1. Webserver reads: dbt_docs_conn_id = "aws_default"
2. Webserver queries Airflow connections for "aws_default"
3. Webserver gets credentials (from DB, secrets backend, or env vars)
4. Webserver creates S3Hook and downloads docs
5. Webserver serves docs to users

No special API integration needed!
"""

# ============================================================================
# STEP 4: Create the Airflow connection
# ============================================================================

"""
Option 1: Via Airflow CLI
--------------------------
airflow connections add aws_default \\
    --conn-type aws \\
    --conn-login AKIAIOSFODNN7EXAMPLE \\
    --conn-password wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \\
    --conn-extra '{"region_name": "us-east-1"}'

Option 2: Via Airflow UI
-------------------------
Admin → Connections → Add Connection
- Connection ID: aws_default
- Connection Type: Amazon Web Services
- AWS Access Key ID: AKIA...
- AWS Secret Access Key: secret...
- Extra: {"region_name": "us-east-1"}

Option 3: Via Environment Variable
-----------------------------------
export AIRFLOW_CONN_AWS_DEFAULT='aws://AKIA...:secret...@?region_name=us-east-1'

Option 4: Via IAM Role (Recommended for AWS deployments)
---------------------------------------------------------
# No credentials needed! Just attach IAM role to pods/instances
Admin → Connections → Add Connection
- Connection ID: aws_default
- Connection Type: Amazon Web Services
- Extra: {"region_name": "us-east-1"}

Option 5: Via Astronomer Cloud
-------------------------------
# Using Astro CLI
astro deployment connection create \\
    --conn-id aws_default \\
    --conn-type aws \\
    --login AKIA... \\
    --password secret... \\
    --extra '{"region_name": "us-east-1"}'

# Or via Astronomer UI
# Deployment → Settings → Connections → Create Connection
"""

# ============================================================================
# Troubleshooting
# ============================================================================

"""
If docs don't load in the webserver:

1. Check connection exists:
   $ airflow connections get aws_default

2. Test S3 access from webserver:
   $ kubectl exec -it <webserver-pod> -- python
   >>> from airflow.providers.amazon.aws.hooks.s3 import S3Hook
   >>> hook = S3Hook(aws_conn_id="aws_default")
   >>> print(hook.list_keys(bucket_name="my-company-dbt-docs", prefix="production/"))

3. Check Cosmos configuration:
   $ kubectl exec -it <webserver-pod> -- python
   >>> from airflow.configuration import conf
   >>> print(conf.get("cosmos", "dbt_docs_dir"))
   >>> print(conf.get("cosmos", "dbt_docs_conn_id"))

4. Check webserver logs:
   $ kubectl logs -f deployment/airflow-webserver | grep -i cosmos

5. Verify files exist in S3:
   $ aws s3 ls s3://my-company-dbt-docs/production/

For more help, see:
- docs/configuration/troubleshooting-docs-access.rst
- docs/configuration/how-webserver-gets-credentials.md
"""

