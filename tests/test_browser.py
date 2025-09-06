import pytest


@pytest.mark.parametrize("target", ["home.spec.ts"])
def test_browser(browser_run, target):
    assert browser_run(target).returncode == 0
