from unittest import mock

pytest_plugins = ["docker_compose"]

from alter_schema.__main__ import Command


@mock.patch("alter_schema.__main__.confirm")
def test_e2e(mock_confirm, module_scoped_container_getter):
    cmd = Command()
    rv = cmd.run(["-H", "localhost", "-u", "test", "-p", "test", "-d", "test", "-t", "test_data",])
    assert rv == 0
    assert mock_confirm.called
