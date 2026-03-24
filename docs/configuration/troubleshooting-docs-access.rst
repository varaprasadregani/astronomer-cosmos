.. _troubleshooting-docs-access:

Troubleshooting dbt Docs Access
================================

This guide helps you diagnose and fix issues with serving dbt docs from cloud storage (S3, GCS, Azure).

Understanding the Architecture
-------------------------------

**Two separate processes access your dbt docs:**

1. **Task Execution** (``DbtDocsS3Operator``)
   
   - Runs in a worker pod/process
   - Uploads docs to cloud storage
   - Uses Airflow connection: ``connection_id`` parameter

2. **Webserver/API Server** (Cosmos Plugin)
   
   - Runs in the webserver pod/process
   - Downloads and serves docs to users
   - Uses Airflow connection: ``dbt_docs_conn_id`` config

**Both use the same Airflow connection system** to get credentials.

Common Issues and Solutions
---------------------------

Issue 1: Docs Upload Successfully but Webserver Can't Access Them
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Symptoms:**

- Task completes successfully
- Files exist in S3/GCS/Azure
- Webserver shows "404 Not Found" or "Permission Denied"

**Cause:** The webserver is using a different connection or no connection.

**Solution:**

Check your Cosmos configuration:

.. code-block:: bash

    # Verify these match
    echo $AIRFLOW__COSMOS__DBT_DOCS_DIR
    echo $AIRFLOW__COSMOS__DBT_DOCS_CONN_ID

Make sure the connection ID matches what you used in the operator:

.. code-block:: python

    # In your DAG
    generate_docs = DbtDocsS3Operator(
        task_id="generate_dbt_docs",
        connection_id="aws_default",  # <-- This connection
        bucket_name="my-bucket",
        ...
    )

.. code-block:: bash

    # In your webserver config (must match)
    export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"

Issue 2: Connection Exists but Webserver Can't Find It
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Symptoms:**

- Connection works in tasks
- ``airflow connections get my_conn`` shows the connection
- Webserver still can't access cloud storage

**Cause:** Webserver process doesn't have access to connections.

**Solution:**

**For Kubernetes/Astronomer:**

Ensure all pods can access connections:

.. code-block:: yaml

    # If using Secrets Backend
    # Make sure webserver has same IAM role/service account as workers
    
    # For Astronomer Cloud:
    # Connections are automatically synced - no action needed

**For Docker Compose:**

Ensure webserver service has access to the database:

.. code-block:: yaml

    services:
      airflow-webserver:
        environment:
          # Same connection to metadata DB as scheduler/workers
          - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://...

**For Local Development:**

Test that the webserver can access the connection:

.. code-block:: bash

    # SSH into webserver container/pod
    kubectl exec -it <webserver-pod> -- /bin/bash
    
    # Or for Docker Compose
    docker exec -it <webserver-container> /bin/bash
    
    # Test connection access
    airflow connections get aws_default
    
    # Test Python access
    python << EOF
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id="aws_default")
    print(hook.get_conn())
    EOF

Issue 3: Different Credentials for Upload vs. Serving
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Scenario:** You want to use different credentials for uploading (task) vs. serving (webserver).

**Solution:**

Use different connection IDs:

.. code-block:: python

    # Task uses one connection (write access)
    generate_docs = DbtDocsS3Operator(
        task_id="generate_dbt_docs",
        connection_id="aws_write_access",  # Full write access
        bucket_name="my-bucket",
        ...
    )

.. code-block:: bash

    # Webserver uses another connection (read-only)
    export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_read_only"

Both connections must point to the same bucket/path.

Issue 4: Astronomer Cloud - Connection Not Working
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**In Astronomer Cloud, connections should "just work" between all components.**

**Verify connection is created:**

.. code-block:: bash

    # Using Astro CLI
    astro deployment connection list
    
    # Or in Airflow UI
    # Admin > Connections

**Verify connection type is correct:**

For S3 docs serving, use:

- Connection Type: ``Amazon Web Services``
- Connection ID: ``aws_default`` (or your custom name)

**Check IAM permissions (if using IAM roles):**

The webserver pod needs ``s3:GetObject`` permission:

.. code-block:: json

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "s3:GetObject",
            "s3:ListBucket"
          ],
          "Resource": [
            "arn:aws:s3:::my-dbt-docs-bucket",
            "arn:aws:s3:::my-dbt-docs-bucket/*"
          ]
        }
      ]
    }

Issue 5: Environment Variables Not Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Symptoms:**

- You set ``AIRFLOW__COSMOS__DBT_DOCS_DIR`` but it's not being read
- Configuration seems to be ignored

**Solution:**

**Restart the webserver** after changing environment variables:

.. code-block:: bash

    # Kubernetes
    kubectl rollout restart deployment airflow-webserver
    
    # Docker Compose
    docker-compose restart airflow-webserver
    
    # Astronomer Cloud
    astro deployment update --force

**Verify environment variables are loaded:**

.. code-block:: bash

    # From inside webserver container
    env | grep AIRFLOW__COSMOS

Diagnostic Checklist
--------------------

Run through this checklist to diagnose issues:

.. code-block:: bash

    # 1. Verify files exist in cloud storage
    aws s3 ls s3://my-bucket/dbt-docs/
    # Should show: index.html, manifest.json, catalog.json
    
    # 2. Verify Cosmos configuration
    python << EOF
    from airflow.configuration import conf
    print("dbt_docs_dir:", conf.get("cosmos", "dbt_docs_dir", fallback="NOT SET"))
    print("dbt_docs_conn_id:", conf.get("cosmos", "dbt_docs_conn_id", fallback="NOT SET"))
    EOF
    
    # 3. Verify connection exists
    airflow connections get aws_default
    
    # 4. Test S3 access from webserver
    python << EOF
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id="aws_default")
    
    # Try to read a file
    content = hook.read_key(
        key="dbt-docs/index.html",
        bucket_name="my-bucket"
    )
    print(f"Successfully read {len(content)} bytes")
    EOF
    
    # 5. Check webserver logs
    kubectl logs -f deployment/airflow-webserver | grep -i cosmos

Working Examples
----------------

Example 1: S3 with IAM Role (Astronomer Cloud)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # DAG
    from cosmos.operators import DbtDocsS3Operator
    
    generate_docs = DbtDocsS3Operator(
        task_id="generate_dbt_docs",
        project_dir="/usr/local/airflow/dags/dbt/my_project",
        profile_config=profile_config,
        connection_id="aws_default",
        bucket_name="my-company-dbt-docs",
        folder_dir="production",
    )

.. code-block:: bash

    # Webserver configuration
    export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-company-dbt-docs/production"
    export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"

Connection in Airflow:

- Connection ID: ``aws_default``
- Connection Type: ``Amazon Web Services``
- Extra: ``{"region_name": "us-east-1"}`` (IAM role provides credentials)

Example 2: S3 with Access Keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Create connection via CLI
    airflow connections add aws_default \
        --conn-type aws \
        --conn-login AKIAIOSFODNN7EXAMPLE \
        --conn-password wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
        --conn-extra '{"region_name": "us-east-1"}'

.. code-block:: bash

    # Webserver configuration
    export AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-bucket/docs"
    export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"

Example 3: GCS with Service Account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # DAG
    from cosmos.operators import DbtDocsGCSOperator
    
    generate_docs = DbtDocsGCSOperator(
        task_id="generate_dbt_docs",
        project_dir="/usr/local/airflow/dags/dbt/my_project",
        profile_config=profile_config,
        connection_id="google_cloud_default",
        bucket_name="my-dbt-docs",
    )

.. code-block:: bash

    # Webserver configuration
    export AIRFLOW__COSMOS__DBT_DOCS_DIR="gs://my-dbt-docs"
    export AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="google_cloud_default"

Connection in Airflow:

- Connection ID: ``google_cloud_default``
- Connection Type: ``Google Cloud``
- Keyfile Path: ``/path/to/service-account.json``

Or use Workload Identity in GKE (no keyfile needed).

Still Having Issues?
--------------------

1. **Check the Cosmos GitHub Issues**: https://github.com/astronomer/astronomer-cosmos/issues
2. **Join Astronomer Community Slack**: https://astronomer.io/slack
3. **Contact Astronomer Support** (for Cloud customers)

When reporting issues, include:

- Airflow version
- Cosmos version
- Deployment type (Astronomer Cloud, local, K8s, etc.)
- Relevant configuration (redact secrets!)
- Webserver logs showing the error

See Also
--------

- :ref:`generating-docs`
- :ref:`hosting-docs`
- :ref:`astronomer-api-integration`

