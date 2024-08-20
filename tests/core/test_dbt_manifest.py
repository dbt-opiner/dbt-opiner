from dbt_opiner.dbt import DbtManifest


def test_dbt_manifest(temp_complete_git_repo):
    manifest = DbtManifest(temp_complete_git_repo / "target" / "manifest.json")
    assert len(manifest.nodes) == 1
    assert len(manifest.macros) == 0
