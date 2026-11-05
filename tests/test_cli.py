"""Tests for the CLI."""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner

from eventbusk import cli


def test_mp_context_is_fork_on_linux() -> None:
    """Linux pins 'fork', regardless of the Python default."""
    with mock.patch("platform.system", return_value="Linux"):
        context = cli._mp_context()  # pylint: disable=protected-access

    assert context is not None
    assert context.get_start_method() == "fork"


def test_mp_context_is_default_on_macos() -> None:
    """MacOS keeps its default start method, as forking there is unsafe."""
    with mock.patch("platform.system", return_value="Darwin"):
        assert cli._mp_context() is None  # pylint: disable=protected-access


@pytest.mark.parametrize("context", [None, mock.sentinel.context])
def test_worker_passes_mp_context_to_manager(context: object) -> None:
    """The worker command hands the chosen context to the ServiceManager."""
    bus = mock.Mock()
    bus.receivers = {mock.Mock()}

    with (
        mock.patch.object(cli, "find_app", return_value=bus),
        mock.patch.object(cli, "_mp_context", return_value=context),
        mock.patch("cotyledon.ServiceManager") as manager_cls,
        mock.patch("eventbusk.bus.EventBus.to_fqn", return_value="r"),
    ):
        result = CliRunner().invoke(cli.worker, ["--app", "mod:app"])

    assert result.exit_code == 0, result.output
    assert manager_cls.call_args.kwargs["mp_context"] is context
    manager_cls.return_value.run.assert_called_once()
