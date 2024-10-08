from unittest import mock

import requests

from dbt_opiner import package

_PYPI_URL = "https://pypi.org/pypi/dbt-opiner/json"


@mock.patch("importlib.metadata.version", return_value="1.0.0")
def test_get_package_version(mock_version):
    assert package.get_package_version() == "1.0.0"
    mock_version.assert_called_once_with("dbt-opiner")


@mock.patch("requests.get")
def test_get_latest_package_version_success(mock_get):
    mock_response = mock_get.return_value
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"info": {"version": "2.0.0"}}

    assert package.get_latest_package_version() == "2.0.0"
    mock_get.assert_called_once_with(_PYPI_URL, timeout=5)


@mock.patch("requests.get")
def test_get_latest_package_version_failure(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException

    assert package.get_latest_package_version() is None
    mock_get.assert_called_once_with(_PYPI_URL, timeout=5)


@mock.patch("click.secho")
@mock.patch("dbt_opiner.package.get_latest_package_version", return_value="2.0.0")
@mock.patch("dbt_opiner.package.get_package_version", return_value="1.0.0")
def test_recommend_version_upgrade_recommends_upgrade(
    mock_get_package_version, mock_get_latest_package_version, mock_click_secho
):
    package.recommend_version_upgrade()

    mock_click_secho.assert_called_once_with(
        "You are using dbt-opiner 1.0.0, however version 2.0.0 is available.\n"
        "Consider upgrading to the latest version.\n",
        fg="yellow",
    )


@mock.patch("click.secho")
@mock.patch("dbt_opiner.package.get_latest_package_version", return_value="1.0.0")
@mock.patch("dbt_opiner.package.get_package_version", return_value="1.0.0")
def test_recommend_version_upgrade_no_upgrade(
    mock_get_package_version, mock_get_latest_package_version, mock_click_secho
):
    package.recommend_version_upgrade()
    mock_click_secho.assert_called_once_with(
        "Using the latest version of dbt-opiner: 1.0.0.", fg="green"
    )


@mock.patch("click.secho")
@mock.patch("dbt_opiner.package.get_latest_package_version", return_value=None)
@mock.patch("dbt_opiner.package.get_package_version", return_value="1.0.0")
def test_recommend_version_upgrade_no_latest_version(
    mock_get_package_version, mock_get_latest_package_version, mock_click_secho
):
    package.recommend_version_upgrade()
    mock_click_secho.assert_not_called()
