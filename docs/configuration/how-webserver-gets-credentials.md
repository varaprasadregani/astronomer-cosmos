# How the Webserver Gets Credentials

## TL;DR

**The webserver gets credentials THE SAME WAY tasks do** - through Airflow's connection system (database/secrets backend). No special API integration is needed for standard deployments.

## Visual Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW DEPLOYMENT                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │  Metadata Database / Secrets Backend                   │    │
│  │  ┌──────────────────────────────────────────────────┐  │    │
│  │  │  Connections:                                     │  │    │
│  │  │  - aws_default (type: aws)                       │  │    │
│  │  │    • login: AKIA...                              │  │    │
│  │  │    • password: secret_key...                     │  │    │
│  │  │    • extra: {"region": "us-east-1"}              │  │    │
│  │  └──────────────────────────────────────────────────┘  │    │
│  └────────────────────────────────────────────────────────┘    │
│                            ↑         ↑                          │
│                            │         │                          │
│            ┌───────────────┘         └───────────────┐          │
│            │                                         │          │
│            │                                         │          │
│  ┌─────────────────────┐                 ┌──────────────────┐  │
│  │  Worker Pod/Process │                 │ Webserver Pod    │  │
│  │  ┌───────────────┐  │                 │ ┌──────────────┐ │  │
│  │  │ Task:         │  │                 │ │ Cosmos Plugin│ │  │
│  │  │ DbtDocsS3Op   │──┼─── Upload ───>  │ │              │ │  │
│  │  │               │  │      docs       │ │ open_s3_file │ │  │
│  │  │ S3Hook(       │  │                 │ │ S3Hook(      │ │  │
│  │  │  "aws_default"│  │                 │ │  "aws_default│ │  │
│  │  │ )             │  │                 │ │ )            │ │  │
│  │  └───────────────┘  │                 │ └──────────────┘ │  │
│  └─────────────────────┘                 └──────────────────┘  │
│            │                                         │          │
│            │                                         │          │
│            └──────────────┐         ┌────────────────┘          │
│                           ↓         ↓                           │
│                    ┌──────────────────┐                         │
│                    │   AWS S3 Bucket  │                         │
│                    │ my-dbt-docs/     │                         │
│                    │  - index.html    │                         │
│                    │  - manifest.json │                         │
│                    │  - catalog.json  │                         │
│                    └──────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

## Step-by-Step Flow

### Phase 1: Task Execution (Upload)

1. **DAG runs** → Task `DbtDocsS3Operator` starts
2. **Operator creates S3Hook** with `connection_id="aws_default"`
3. **S3Hook queries** Airflow's connection system:
   - Checks metadata database
   - Checks secrets backend (if configured)
   - Checks environment variables (`AIRFLOW_CONN_AWS_DEFAULT`)
4. **Hook gets credentials** and uploads docs to S3
5. **Files now in S3**: `s3://my-bucket/dbt-docs/index.html`, etc.

### Phase 2: Webserver Serving (Download)

1. **User visits** `/cosmos/dbt_docs` in Airflow UI
2. **Cosmos plugin** needs to serve `index.html`, `manifest.json`
3. **Plugin reads config**:
   ```python
   dbt_docs_dir = conf.get("cosmos", "dbt_docs_dir")  # "s3://my-bucket/dbt-docs"
   conn_id = conf.get("cosmos", "dbt_docs_conn_id")   # "aws_default"
   ```
4. **Plugin creates S3Hook** with `aws_conn_id="aws_default"`
5. **S3Hook queries** Airflow's connection system (SAME as tasks do):
   - Checks metadata database
   - Checks secrets backend (if configured)
   - Checks environment variables
6. **Hook gets credentials** and downloads file from S3
7. **Plugin serves content** to user's browser

## Configuration Examples

### Example 1: Using Airflow Connection (Standard)

```bash
# Step 1: Create connection (via UI, CLI, or env var)
airflow connections add aws_default \
    --conn-type aws \
    --conn-login AKIAIOSFODNN7EXAMPLE \
    --conn-password wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
    --conn-extra '{"region_name": "us-east-1"}'

# Step 2: Configure DAG to upload
```

```python
# dags/generate_docs.py
from cosmos.operators import DbtDocsS3Operator

generate_docs = DbtDocsS3Operator(
    task_id="generate_dbt_docs",
    connection_id="aws_default",  # ← Uses this connection
    bucket_name="my-company-dbt-docs",
)
```

```bash
# Step 3: Configure webserver to serve
export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-company-dbt-docs"
export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"  # ← Uses same connection
```

**Result**: Both task and webserver use the same connection → Everything works!

### Example 2: Using IAM Roles (Astronomer Cloud / AWS EKS)

```bash
# No explicit credentials needed!
# Connection just specifies the region
```

Connection in Airflow UI:
- Connection ID: `aws_default`
- Connection Type: `Amazon Web Services`
- Extra: `{"region_name": "us-east-1"}`
- Login: *(leave empty)*
- Password: *(leave empty)*

Both worker pods and webserver pods have IAM roles attached that allow S3 access.

### Example 3: Using AWS Secrets Manager

```bash
# Configure Airflow to use Secrets Manager
export AIRFLOW__SECRETS__BACKEND="airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
export AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "airflow/connections"}'

# Store connection in Secrets Manager
aws secretsmanager create-secret \
    --name airflow/connections/aws_default \
    --secret-string '{
        "conn_type": "aws",
        "login": "AKIA...",
        "password": "secret...",
        "extra": {"region_name": "us-east-1"}
    }'
```

Both tasks and webserver query Secrets Manager to get credentials.

## Astronomer Cloud Specifics

In Astronomer Cloud:

1. **Connections are synced automatically** across all components (webserver, scheduler, workers)
2. **Create connections** via:
   - Astronomer UI (Settings → Connections)
   - Astro CLI: `astro deployment connection create`
   - Airflow UI (Admin → Connections)
3. **IAM roles** can be attached to deployments for passwordless authentication

```bash
# List connections in your deployment
astro deployment connection list

# Create connection
astro deployment connection create \
    --conn-id aws_default \
    --conn-type aws \
    --login AKIA... \
    --password secret... \
    --extra '{"region_name": "us-east-1"}'
```

## When Would You Use the Astronomer Platform API?

You would **rarely** need the Platform API integration for serving docs. It's only useful if:

1. You're building a **custom external application** that needs to access Airflow connections
2. You're **migrating credentials** between deployments programmatically
3. You have **compliance requirements** to fetch credentials dynamically on every request

**For normal dbt docs serving, just use standard Airflow connections!**

## Debugging Connection Access

```python
# Run this in both a task and in the webserver to verify connection access
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def test_connection():
    hook = S3Hook(aws_conn_id="aws_default")
    conn = hook.get_connection("aws_default")
    
    print(f"Connection ID: {conn.conn_id}")
    print(f"Connection Type: {conn.conn_type}")
    print(f"Login: {conn.login[:4]}..." if conn.login else "None")
    print(f"Host: {conn.host}")
    print(f"Extra: {conn.extra}")
    
    # Try to list bucket
    s3_client = hook.get_conn()
    response = s3_client.list_buckets()
    print(f"Successfully connected! Found {len(response['Buckets'])} buckets")

# In a task:
test_connection()  # Should work

# In webserver (via python shell):
test_connection()  # Should also work if configured correctly
```

## Common Misconceptions

### ❌ "The webserver can't access Airflow connections"

**False!** The webserver is part of the Airflow deployment and has full access to connections.

### ❌ "I need to use the Platform API to serve docs"

**False!** The Platform API is for external applications or special cases, not for standard Cosmos usage.

### ❌ "Connections work in tasks but not in the webserver"

**Rare!** If this happens, it's usually a configuration issue:
- Webserver environment variables not set
- Webserver doesn't have network access to secrets backend
- Connection was created in a different scope

### ✅ "Both tasks and webserver use the same connection system"

**Correct!** They both query the same sources:
1. Airflow metadata database
2. Secrets backend
3. Environment variables

## Summary

**You don't need special API integration for Cosmos to serve dbt docs from S3/GCS/Azure.**

Just:
1. ✅ Create an Airflow connection (UI, CLI, or env var)
2. ✅ Use that connection in `DbtDocsS3Operator`
3. ✅ Configure `dbt_docs_conn_id` to use the same connection
4. ✅ Done!

The webserver gets credentials automatically through Airflow's built-in connection system.

