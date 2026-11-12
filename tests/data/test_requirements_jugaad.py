"""Bug 9: jugaad-data missing from requirements.txt.

master_derivatives_fetch.py imports jugaad_data at the top level.
A clean `pip install -r requirements.txt` therefore fails at import time.
Expected fix: add jugaad-data (with its current pinned version) to requirements.txt.
"""
from pathlib import Path

REQUIREMENTS_PATH = Path(__file__).resolve().parents[2] / "requirements.txt"


def _get_requirement_names() -> set[str]:
    """Return the set of package names (lowercase, dashes normalised) from requirements.txt."""
    names = set()
    for line in REQUIREMENTS_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # strip version specifier: jugaad-data==0.29 → jugaad-data
        name = line.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("[")[0].strip()
        names.add(name.lower().replace("_", "-"))
    return names


class TestJugaadDataInRequirements:

    def test_jugaad_data_listed_in_requirements(self):
        names = _get_requirement_names()
        assert "jugaad-data" in names, (
            "jugaad-data is imported by master_derivatives_fetch.py but is "
            "absent from requirements.txt — a clean pip install will fail"
        )

    def test_jugaad_data_has_version_pin(self):
        for line in REQUIREMENTS_PATH.read_text().splitlines():
            line = line.strip()
            if line.lower().startswith("jugaad-data") or line.lower().startswith("jugaad_data"):
                assert "==" in line, (
                    f"jugaad-data in requirements.txt should have a pinned version (==), got: {line!r}"
                )
                return
        # if we get here, the package wasn't found — first test already catches this
        assert False, "jugaad-data not found in requirements.txt"

    def test_jugaad_data_importable(self):
        """Smoke-test: the installed package must be importable."""
        from jugaad_data.nse import bhavcopy_fo_save  # noqa: F401
