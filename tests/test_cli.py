"""Tests for the CLI."""

from __future__ import annotations

from unittest import mock

from click.testing import CliRunner

from eventbusk import cli


def test_worker_uses_fork_start_method() -> None:
    """The service manager must pin 'fork', regardless of the Python default."""
    bus = mock.Mock()
    bus.receivers = {mock.Mock()}

    with (
        mock.patch.object(cli, "find_app", return_value=bus),
        mock.patch.object(cli.cotyledon, "ServiceManager") as manager_cls,
        mock.patch.object(cli.EventBus, "to_fqn", return_value="r"),
    ):
        result = CliRunner().invoke(cli.worker, ["--app", "mod:app"])

    assert result.exit_code == 0, result.output
    mp_context = manager_cls.call_args.kwargs["mp_context"]
    assert mp_context.get_start_method() == "fork"
    manager_cls.return_value.run.assert_called_once()
