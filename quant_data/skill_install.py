from __future__ import annotations

import shutil
from pathlib import Path

from quant_data.models import SkillInstallResult

SKILL_NAME = "quant-data"
PACKAGED_SKILL_RELATIVE_PATH = Path("resources") / "skills" / SKILL_NAME
REPO_SKILL_RELATIVE_PATH = Path("skills") / SKILL_NAME
CODEX_AGENTS_RELATIVE_PATH = Path("agents") / f"{SKILL_NAME}.yaml"


class SkillInstallError(ValueError):
    pass


def install_skill(runtime_root: str | Path, *, codex: bool = False) -> SkillInstallResult:
    source = find_skill_source()
    destination_root = Path(runtime_root).expanduser().resolve()
    skills_dest = destination_root / "skills" / SKILL_NAME
    agents_config_path = destination_root / CODEX_AGENTS_RELATIVE_PATH if codex else None

    files_copied = copy_skill_tree(source, skills_dest)
    if codex and agents_config_path is not None:
        copy_codex_agents_config(source, agents_config_path)

    return SkillInstallResult(
        runtime_root=destination_root,
        skill_name=SKILL_NAME,
        skill_source=source,
        skills_dest=skills_dest,
        files_copied=files_copied,
        codex=codex,
        agents_config_path=agents_config_path,
    )


def find_skill_source() -> Path:
    installed_candidate = Path(__file__).resolve().parent / PACKAGED_SKILL_RELATIVE_PATH
    if is_valid_skill_dir(installed_candidate):
        return installed_candidate

    for parent in Path(__file__).resolve().parents:
        repo_candidate = parent / REPO_SKILL_RELATIVE_PATH
        if is_valid_skill_dir(repo_candidate):
            return repo_candidate.resolve()

    raise SkillInstallError(f"skill '{SKILL_NAME}' not found in package resources")


def is_valid_skill_dir(path: Path) -> bool:
    return (path / "SKILL.md").is_file()


def copy_skill_tree(source: Path, destination: Path) -> int:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)
    return sum(1 for item in destination.rglob("*") if item.is_file())


def copy_codex_agents_config(source: Path, destination: Path) -> None:
    source_yaml = source / "agents" / "openai.yaml"
    if not source_yaml.is_file():
        raise SkillInstallError(f"missing Codex agents config in '{source}'")

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_yaml, destination)
