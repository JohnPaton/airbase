import pytest

import airbase


@pytest.mark.skipif(airbase.__version__ is None, reason="package not installed")
def test_version_well_defined():
    assert 3 <= len(airbase.__version__.split(".")) <= 5
