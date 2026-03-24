"""
Utility module for interacting with Astronomer Platform API.

This module provides functionality to fetch environment objects (connections)
from Astronomer Cloud using the Platform API.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


class AstronomerAPIError(Exception):
    """Exception raised for errors in the Astronomer API."""

    pass


def get_astronomer_api_token() -> Optional[str]:
    """
    Get the Astronomer API token from environment variables.

    Looks for token in the following order:
    1. ASTRONOMER_API_TOKEN
    2. ASTRO_API_TOKEN

    Returns:
        The API token if found, None otherwise.
    """
    return os.getenv("ASTRONOMER_API_TOKEN") or os.getenv("ASTRO_API_TOKEN")


def get_environment_object(
    organization_id: str, environment_object_id: str, api_token: Optional[str] = None
) -> dict[str, Any]:
    """
    Retrieve an environment object (connection) from Astronomer Platform API.

    Args:
        organization_id: The ID of the Organization to which the environment object belongs.
        environment_object_id: The environment object's ID.
        api_token: Bearer token for authentication. If not provided, will look for
                   ASTRONOMER_API_TOKEN or ASTRO_API_TOKEN environment variables.

    Returns:
        The environment object as a dictionary.

    Raises:
        AstronomerAPIError: If the API request fails.

    Example:
        >>> env_obj = get_environment_object(
        ...     organization_id="org123",
        ...     environment_object_id="env456"
        ... )
        >>> connection_details = env_obj.get("connection", {})
    """
    if api_token is None:
        api_token = get_astronomer_api_token()

    if not api_token:
        raise AstronomerAPIError(
            "No Astronomer API token found. Please set ASTRONOMER_API_TOKEN or ASTRO_API_TOKEN environment variable."
        )

    url = f"https://api.astronomer.io/platform/v1beta1/organizations/{organization_id}/environment-objects/{environment_object_id}"

    headers = {"Authorization": f"Bearer {api_token}"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise AstronomerAPIError("Unauthorized: Invalid or expired API token") from e
        elif e.response.status_code == 403:
            raise AstronomerAPIError(
                f"Forbidden: You don't have permission to access environment object {environment_object_id}"
            ) from e
        elif e.response.status_code == 404:
            raise AstronomerAPIError(
                f"Not Found: Environment object {environment_object_id} not found in organization {organization_id}"
            ) from e
        else:
            raise AstronomerAPIError(f"HTTP error {e.response.status_code}: {e.response.text}") from e
    except requests.exceptions.RequestException as e:
        raise AstronomerAPIError(f"Failed to connect to Astronomer API: {str(e)}") from e


def get_connection_from_environment_object(
    organization_id: str, connection_key: str, api_token: Optional[str] = None
) -> dict[str, Any]:
    """
    Retrieve connection details from an environment object by connection key (conn_id).

    This is a convenience method that searches for an environment object by its objectKey
    (which corresponds to the Airflow connection ID) and returns the connection details.

    Note: This method requires listing environment objects first, which is not directly
    supported by a single API endpoint. In practice, you should cache the environment_object_id
    or use the get_environment_object method directly if you know the ID.

    Args:
        organization_id: The ID of the Organization.
        connection_key: The connection key (Airflow conn_id).
        api_token: Bearer token for authentication.

    Returns:
        Connection details dictionary containing host, login, password, etc.

    Raises:
        AstronomerAPIError: If the connection cannot be found or retrieved.
    """
    # Note: The Platform API doesn't provide a direct endpoint to search by objectKey
    # This is a placeholder implementation. In production, you'd need to:
    # 1. Cache the environment_object_id mapping to conn_id
    # 2. Or use the list endpoint to find the object (if available)
    # 3. Or configure the environment_object_id directly in Cosmos settings

    raise NotImplementedError(
        "Searching environment objects by connection key requires the list endpoint "
        "or cached mapping. Please use get_environment_object() with the environment_object_id directly."
    )


def build_s3_hook_from_environment_object(env_object: dict[str, Any]) -> Any:
    """
    Build an S3Hook from Astronomer environment object connection details.

    Args:
        env_object: The environment object dictionary from the Platform API.

    Returns:
        An initialized S3Hook.

    Raises:
        AstronomerAPIError: If the environment object is not a CONNECTION type or
                           doesn't contain valid S3 connection details.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    if env_object.get("objectType") != "CONNECTION":
        raise AstronomerAPIError(f"Environment object is not a CONNECTION type: {env_object.get('objectType')}")

    connection_details = env_object.get("connection", {})
    if not connection_details:
        raise AstronomerAPIError("No connection details found in environment object")

    # Build connection kwargs from environment object
    conn_kwargs = {}

    # Extract connection parameters
    if "login" in connection_details:
        conn_kwargs["aws_access_key_id"] = connection_details["login"]
    if "password" in connection_details:
        conn_kwargs["aws_secret_access_key"] = connection_details["password"]

    # Handle extra parameters (region, etc.)
    extra = connection_details.get("extra", {})
    if extra:
        if isinstance(extra, dict):
            if "region_name" in extra:
                conn_kwargs["region_name"] = extra["region_name"]
        # Note: The actual S3Hook initialization might require Connection object
        # This is a simplified implementation

    logger.info("Built S3Hook from Astronomer environment object")

    # For now, return a basic S3Hook
    # In production, you might need to create a Connection object first
    return S3Hook(**conn_kwargs)


def create_airflow_connection_from_environment_object(env_object: dict[str, Any]) -> Any:
    """
    Create an Airflow Connection object from Astronomer environment object.

    Args:
        env_object: The environment object dictionary from the Platform API.

    Returns:
        An Airflow Connection object.

    Raises:
        AstronomerAPIError: If the environment object is not a CONNECTION type.
    """
    from airflow.models import Connection

    if env_object.get("objectType") != "CONNECTION":
        raise AstronomerAPIError(f"Environment object is not a CONNECTION type: {env_object.get('objectType')}")

    connection_details = env_object.get("connection", {})
    if not connection_details:
        raise AstronomerAPIError("No connection details found in environment object")

    # Build Connection object
    conn = Connection(
        conn_id=env_object.get("objectKey", "unknown"),
        conn_type=connection_details.get("type", ""),
        host=connection_details.get("host"),
        login=connection_details.get("login"),
        password=connection_details.get("password"),
        schema=connection_details.get("schema"),
        port=connection_details.get("port"),
        extra=connection_details.get("extra", {}),
    )

    return conn

