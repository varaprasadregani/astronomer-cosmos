.. _astronomer-api-integration:

Astronomer Platform API Integration
====================================

This guide explains how Cosmos webserver gets credentials to serve dbt docs from cloud storage, and when you might need to use the Astronomer Platform API.

Standard Configuration (Recommended)
------------------------------------

In most cases, including Astronomer Cloud, the webserver accesses credentials the same way tasks do - through Airflow's connection system.

**Configuration:**

.. code-block:: bash

    # Set where the dbt docs are located (S3, GCS, Azure, etc.)
    export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-bucket/path/to/dbt-docs"
    
    # Set which Airflow connection to use
    export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"

**How it works:**

1. ``DbtDocsS3Operator`` uploads docs using the configured AWS connection
2. The webserver plugin reads docs using the same connection
3. Both access the connection through Airflow's standard connection system:
   
   - Airflow metadata database
   - Secrets backend (AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault, etc.)
   - Environment variables (``AIRFLOW_CONN_*``)

This works in:

- Local Airflow installations
- Astronomer Cloud (connections are synced to all components)
- Kubernetes deployments
- Docker Compose setups

When to Use Astronomer Platform API
------------------------------------

The Astronomer Platform API integration is only needed in special cases where:

1. **Dynamic Connection Retrieval**: You need to fetch connection details at runtime from the Astronomer Platform API rather than using Airflow's connection system
2. **Separate Credential Management**: You're managing credentials separately from Airflow connections
3. **External Service Integration**: A service outside of Airflow needs to access connections

**This is NOT needed for standard dbt docs serving in Astronomer Cloud.**

Configuration (Advanced)
-------------------------

If you need to use the Platform API:

.. code-block:: bash

    # Enable Astronomer API integration
    export AIRFLOW__COSMOS__USE_ASTRONOMER_API_FOR_CONNECTIONS="true"
    
    # Your Astronomer organization ID
    export AIRFLOW__COSMOS__ASTRONOMER_ORGANIZATION_ID="clxxxxxxxxxxxxxx"
    
    # The environment object ID for your connection
    export AIRFLOW__COSMOS__ASTRONOMER_ENVIRONMENT_OBJECT_ID="clxxxxxxxxxxxxxx"
    
    # API token for authentication
    export ASTRONOMER_API_TOKEN="your-api-token-here"

**Finding your Organization ID:**

1. Go to https://cloud.astronomer.io/
2. Navigate to your organization settings
3. Copy the Organization ID

**Finding your Environment Object ID:**

Use the Astronomer CLI or API to list environment objects:

.. code-block:: bash

    astro environment-object list --organization-id <org-id>

**Creating an API Token:**

1. Go to https://cloud.astronomer.io/
2. Navigate to **Settings** > **Access Tokens**
3. Create a new token with ``environment:read`` permissions

How the API Integration Works
------------------------------

When enabled, Cosmos will:

1. Detect S3/GCS/Azure URL in ``dbt_docs_dir``
2. Make an API call to Astronomer Platform:

   .. code-block:: python

       GET https://api.astronomer.io/platform/v1beta1/organizations/{orgId}/environment-objects/{objectId}

3. Parse the connection details from the response
4. Create an Airflow connection object dynamically
5. Use that connection to access cloud storage

API Response Example
--------------------

The Platform API returns connection details in this format:

.. code-block:: json

    {
      "id": "clxxxxxxxxxxxxxx",
      "objectKey": "aws_docs_connection",
      "objectType": "CONNECTION",
      "connection": {
        "type": "aws",
        "login": "AKIAIOSFODNN7EXAMPLE",
        "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "extra": {
          "region_name": "us-east-1"
        }
      }
    }

Troubleshooting
---------------

**Error: "No Astronomer API token found"**

Make sure you've set the API token:

.. code-block:: bash

    export ASTRONOMER_API_TOKEN="your-token"
    # or
    export ASTRO_API_TOKEN="your-token"

**Error: "Unauthorized: Invalid or expired API token"**

Your API token may have expired. Create a new one in the Astronomer UI.

**Error: "Not Found: Environment object not found"**

- Verify your organization ID is correct
- Verify your environment object ID is correct
- Make sure the environment object exists and is of type ``CONNECTION``

**Docs still not loading**

Try the standard approach first:

1. Disable API integration: ``export AIRFLOW__COSMOS__USE_ASTRONOMER_API_FOR_CONNECTIONS="false"``
2. Ensure your Airflow connection exists: ``airflow connections get aws_default``
3. Test S3 access from the webserver pod/container

Best Practices
--------------

1. **Use Standard Approach First**: The Airflow connection system is more reliable and doesn't require API calls on every request
2. **Cache API Responses**: If using the API integration, consider caching responses to avoid rate limits
3. **Monitor API Usage**: The Platform API has rate limits; excessive calls may be throttled
4. **Secure Your Tokens**: Store API tokens in secrets management, not in code or environment variables in production

Example DAG Configuration
-------------------------

Here's a complete example of configuring dbt docs with S3:

.. code-block:: python

    from cosmos.operators import DbtDocsS3Operator
    from cosmos import ProfileConfig
    from airflow import DAG
    from datetime import datetime

    profile_config = ProfileConfig(
        profile_name="my_dbt_profile",
        target_name="prod",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
            profile_args={"database": "my_db", "schema": "my_schema"},
        )
    )

    with DAG(
        dag_id="generate_dbt_docs",
        start_date=datetime(2024, 1, 1),
        schedule=None,
    ) as dag:
        
        generate_docs = DbtDocsS3Operator(
            task_id="generate_dbt_docs",
            project_dir="/path/to/dbt/project",
            profile_config=profile_config,
            # S3 configuration
            connection_id="aws_default",  # Uses Airflow connection
            bucket_name="my-dbt-docs-bucket",
            folder_dir="prod/docs",  # Optional: organize by environment
        )

Then configure the webserver to serve these docs:

.. code-block:: bash

    # In airflow.cfg or environment variables
    [cosmos]
    dbt_docs_dir = s3://my-dbt-docs-bucket/prod/docs
    dbt_docs_conn_id = aws_default

The webserver will automatically use the same ``aws_default`` connection to read the docs.

See Also
--------

- `Astronomer Platform API Documentation <https://www.astronomer.io/docs/astro/astro-api/platform-api-reference/environment/get-environment-object>`_
- :ref:`generating-docs`
- :ref:`hosting-docs`

