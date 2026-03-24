# How Cosmos Webserver Gets Credentials to Serve dbt Docs from S3

## Quick Answer

**The webserver gets credentials the SAME WAY tasks do** - through Airflow's standard connection system (database/secrets backend/environment variables).

You don't need any special API integration for standard deployments!

## Setup (3 Simple Steps)

### 1. Create an Airflow Connection

```bash
# Via CLI
airflow connections add aws_default \
    --conn-type aws \
    --conn-login AKIA... \
    --conn-password secret... \
    --conn-extra '{"region_name": "us-east-1"}'

# Or via Airflow UI: Admin → Connections → Add
# Or via Astronomer Cloud: Deployment → Settings → Connections
```

### 2. Upload docs with DbtDocsS3Operator

```python
from cosmos.operators import DbtDocsS3Operator

generate_docs = DbtDocsS3Operator(
    task_id="generate_dbt_docs",
    connection_id="aws_default",  # ← Uses Airflow connection
    bucket_name="my-dbt-docs",
    ...
)
```

### 3. Configure webserver to serve those docs

```bash
export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-dbt-docs"
export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"  # ← Same connection!
```

**That's it!** The webserver will automatically use the connection to read and serve docs.

## How It Works

```
                Airflow Connection System
                (Database/Secrets Backend)
                          ↑
                          │
              ┌───────────┴───────────┐
              │                       │
         Task queries            Webserver queries
         connection              connection
              │                       │
              ↓                       ↓
    DbtDocsS3Operator          Cosmos Plugin
    Uploads to S3       ←───   Downloads from S3
```

Both task and webserver query the same connection system!

## Files Created

I've created several resources to help you:

### 1. **Astronomer API Integration** (Advanced - rarely needed)
   - `cosmos/_utils/astronomer_api.py` - Utility to fetch connections from Astronomer Platform API
   - `docs/configuration/astronomer-api-integration.rst` - When and how to use it
   - **Note**: This is only needed for special cases, not standard doc serving!

### 2. **Troubleshooting Guide** (Read this if you have issues)
   - `docs/configuration/troubleshooting-docs-access.rst`
   - Covers common issues and solutions
   - Includes diagnostic checklist

### 3. **Conceptual Guide** (Understand how it works)
   - `docs/configuration/how-webserver-gets-credentials.md`
   - Visual diagrams and step-by-step explanation
   - Clears up common misconceptions

### 4. **Working Example** (Copy-paste ready)
   - `examples/docs_serving_example.py`
   - Complete DAG example with comments
   - Multiple connection configuration options

## Common Scenarios

### Scenario 1: Astronomer Cloud (Most Common)

```bash
# Create connection in Astronomer UI or CLI
astro deployment connection create \
    --conn-id aws_default \
    --conn-type aws \
    --extra '{"region_name": "us-east-1"}'
    # IAM role provides credentials automatically

# Configure webserver
export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-bucket/docs"
export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"
```

✅ Connection automatically synced to all components (webserver, scheduler, workers)

### Scenario 2: Local/Self-Hosted with AWS Keys

```bash
# Create connection
airflow connections add aws_default \
    --conn-type aws \
    --conn-login AKIA... \
    --conn-password secret...

# Configure webserver
export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-bucket/docs"
export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"
```

✅ Connection stored in Airflow database, accessible to all components

### Scenario 3: Using AWS Secrets Manager

```bash
# Configure secrets backend
export AIRFLOW__SECRETS__BACKEND="airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"

# Store connection in Secrets Manager
aws secretsmanager create-secret \
    --name airflow/connections/aws_default \
    --secret-string '{"conn_type":"aws",...}'

# Configure webserver
export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-bucket/docs"
export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"
```

✅ Both task and webserver query Secrets Manager for credentials

## Do I Need the Astronomer Platform API Integration?

**99% of the time: NO!**

The Platform API integration (`cosmos/_utils/astronomer_api.py`) is only needed if:

- ❌ You're serving dbt docs from S3/GCS/Azure → **Use standard connections**
- ❌ You're in Astronomer Cloud → **Use standard connections (they sync automatically)**
- ❌ You want tasks and webserver to use the same connection → **They already do!**
- ✅ You're building a custom external app that needs Airflow connection details
- ✅ You have compliance requirements to fetch credentials dynamically on every request

**For serving dbt docs, just use standard Airflow connections!**

## Troubleshooting

If docs don't load:

```bash
# 1. Verify connection exists
airflow connections get aws_default

# 2. Verify Cosmos config
python -c "from airflow.configuration import conf; \
    print('dbt_docs_dir:', conf.get('cosmos', 'dbt_docs_dir', fallback='NOT SET')); \
    print('dbt_docs_conn_id:', conf.get('cosmos', 'dbt_docs_conn_id', fallback='NOT SET'))"

# 3. Test S3 access from webserver
kubectl exec -it <webserver-pod> -- python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id='aws_default')
print(hook.list_keys(bucket_name='my-bucket', prefix='docs/'))
"

# 4. Restart webserver after config changes
kubectl rollout restart deployment airflow-webserver
```

See `docs/configuration/troubleshooting-docs-access.rst` for detailed troubleshooting.

## Key Takeaways

1. ✅ **Webserver uses Airflow connections** (same as tasks)
2. ✅ **No special API integration needed** for standard deployments
3. ✅ **Same connection ID** for both upload and serving
4. ✅ **Works with**: IAM roles, access keys, secrets backends
5. ✅ **Automatic in Astronomer Cloud** (connections sync everywhere)

## Next Steps

1. Read: `examples/docs_serving_example.py` for a complete working example
2. If issues: `docs/configuration/troubleshooting-docs-access.rst`
3. To understand deeply: `docs/configuration/how-webserver-gets-credentials.md`

## Questions?

- GitHub Issues: https://github.com/astronomer/astronomer-cosmos/issues
- Astronomer Slack: https://astronomer.io/slack
- Docs: https://astronomer.github.io/astronomer-cosmos/

