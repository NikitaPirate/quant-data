from __future__ import annotations

from pathlib import Path

from quant_data import skill_install


def test_install_skill_copies_tree_and_codex_agents_config(tmp_path: Path) -> None:
    runtime_root = tmp_path / "codex-home"

    result = skill_install.install_skill(runtime_root, codex=True)

    assert result.skill_name == "quant-data"
    assert result.skills_dest == runtime_root / "skills" / "quant-data"
    assert result.skills_dest.exists()
    assert (result.skills_dest / "SKILL.md").exists()
    assert (result.skills_dest / "agents" / "openai.yaml").exists()
    assert result.agents_config_path == runtime_root / "agents" / "quant-data.yaml"
    assert result.agents_config_path is not None
    assert result.agents_config_path.exists()
    assert result.files_copied >= 2


def test_install_skill_overwrites_existing_files(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    destination = runtime_root / "skills" / "quant-data"
    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("stale")
    (destination / "obsolete.txt").write_text("remove me")

    result = skill_install.install_skill(runtime_root)

    assert result.skills_dest == destination
    assert (destination / "SKILL.md").read_text() != "stale"
    assert not (destination / "obsolete.txt").exists()


def test_find_skill_source_returns_repo_skill() -> None:
    source = skill_install.find_skill_source()

    assert source.name == "quant-data"
    assert (source / "SKILL.md").exists()
