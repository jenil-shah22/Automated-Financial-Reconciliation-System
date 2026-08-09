"""Shared fixtures.

Data is generated once per test session into a temp directory. Tests never
touch data/raw/ in the repo, so running pytest can never leave the working
copy in a state that differs from a clean generator run.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ledgerlens import generate_data

SEED = 42


@pytest.fixture(scope="session")
def generated(tmp_path_factory) -> Path:
    """Generate the full dataset once; yield the directory holding it."""
    out_dir = tmp_path_factory.mktemp("raw")
    generate_data.generate(seed=SEED, out_dir=out_dir)
    return out_dir


@pytest.fixture(scope="session")
def manifest(generated: Path) -> dict:
    import json

    with open(generated / "control_manifest.json", "r", encoding="utf-8") as fh:
        return json.load(fh)
