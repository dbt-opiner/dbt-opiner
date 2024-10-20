from importlib import metadata
from typing import Optional

import click
import requests
from packaging import version

_PYPI_URL = "https://pypi.org/pypi/dbt-opiner/json"


def get_package_version() -> str:
    return metadata.version("dbt-opiner")


def get_latest_package_version() -> Optional[str]:
    try:
        resp = requests.get(_PYPI_URL, timeout=5)
        resp.raise_for_status()
        ver: Optional[str] = resp.json().get("info", {}).get("version")
        return ver
    except Exception:
        return None


def recommend_version_upgrade() -> None:
    latest_version = get_latest_package_version()
    current_version = get_package_version()

    if not latest_version:
        # Failed to obtain the latest version, so skip the check
        return
    current_version_package = version.parse(current_version)
    latest_version_package = version.parse(latest_version)

    if current_version_package < latest_version_package:
        click.secho(
            f"You are using dbt-opiner {current_version}, however version {latest_version} is available.\n"
            f"Consider upgrading to the latest version.\n",
            fg="yellow",
        )
    elif current_version_package == latest_version_package:
        click.secho(
            f"Using the latest version of dbt-opiner: {current_version}.", fg="green"
        )
