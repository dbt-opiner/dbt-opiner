import os

from dbt_opiner import dbt


def test_dbt_manifest(temp_complete_git_repo):
    os.chdir(temp_complete_git_repo)
    manifest = dbt.DbtManifest(
        temp_complete_git_repo / "dbt_project" / "target" / "manifest.json"
    )
    assert len(manifest.nodes.values()) == 1
    assert len(manifest.macros.values()) == 1
    assert len(manifest.sources.values()) == 1
