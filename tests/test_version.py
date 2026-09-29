"""Version bookkeeping: one version, and a changelog entry for it."""

import re
from pathlib import Path

import s2snoop

ROOT = Path(__file__).resolve().parent.parent


def test_version_is_semver_and_single_sourced():
    # regex rather than tomllib: tomllib needs Python 3.11 and we support 3.10
    version = re.search(r'^version = "([^"]+)"$', (ROOT / "pyproject.toml").read_text(), re.M).group(1)
    assert re.fullmatch(r"\d+\.\d+\.\d+", version)
    assert s2snoop.__version__ == version


def test_changelog_has_current_version():
    version = s2snoop.__version__
    changelog = (ROOT / "CHANGELOG.md").read_text()
    assert "## [Unreleased]" in changelog
    assert re.search(rf"^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}$", changelog, re.M)
    assert f"[{version}]: https://github.com/" in changelog
