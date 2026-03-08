"""Tests for the tmux background utility."""

import os
import shutil
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from engine.utils.background import maybe_background


class TestMaybeBackground:
    """Unit tests for maybe_background() — no actual tmux spawning."""

    def test_no_flag_returns_immediately(self):
        """Without --background in sys.argv, function is a no-op."""
        original = sys.argv.copy()
        sys.argv = ["script.py", "--spec", "foo.yaml"]
        try:
            maybe_background("test")  # Should return without side effects
        finally:
            sys.argv = original

    def test_flag_stripped_when_in_tmux(self):
        """Inside tmux, --background is stripped and function returns."""
        original = sys.argv.copy()
        sys.argv = ["script.py", "--background", "--spec", "foo.yaml"]
        try:
            with patch.dict(os.environ, {"TMUX": "/tmp/tmux-1000/default,12345,0"}):
                maybe_background("test")
            assert "--background" not in sys.argv
            assert sys.argv == ["script.py", "--spec", "foo.yaml"]
        finally:
            sys.argv = original

    def test_no_tmux_binary_warns_and_continues(self):
        """If tmux is not installed, warn and run in foreground."""
        original = sys.argv.copy()
        sys.argv = ["script.py", "--background", "--spec", "foo.yaml"]
        try:
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop("TMUX", None)
                with patch("shutil.which", return_value=None):
                    maybe_background("test")
            # --background should still be stripped
            assert "--background" not in sys.argv
        finally:
            sys.argv = original

    def test_launches_tmux_and_exits(self, tmp_path):
        """Outside tmux with --background, launches tmux session and exits."""
        original = sys.argv.copy()
        sys.argv = ["script.py", "--background", "--name", "myreview"]
        try:
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop("TMUX", None)
                mock_run = MagicMock()
                with patch("shutil.which", return_value="/usr/bin/tmux"):
                    with patch("subprocess.run", mock_run):
                        with pytest.raises(SystemExit) as exc_info:
                            maybe_background("screening", review_name="myreview")

            assert exc_info.value.code == 0

            # Verify tmux was called correctly
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert call_args[0] == "tmux"
            assert call_args[1] == "new-session"
            assert call_args[2] == "-d"
            assert call_args[3] == "-s"
            session_name = call_args[4]
            assert session_name.startswith("ee_screening_")
        finally:
            sys.argv = original

    def test_session_name_format(self, tmp_path):
        """Session name follows ee_{stage}_{YYYYMMDD_HHMMSS} pattern."""
        import re

        original = sys.argv.copy()
        sys.argv = ["script.py", "--background"]
        try:
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop("TMUX", None)
                mock_run = MagicMock()
                with patch("shutil.which", return_value="/usr/bin/tmux"):
                    with patch("subprocess.run", mock_run):
                        with pytest.raises(SystemExit):
                            maybe_background("extraction", review_name="test_review")

            session_name = mock_run.call_args[0][0][4]
            pattern = r"^ee_extraction_\d{8}_\d{6}$"
            assert re.match(pattern, session_name), f"Bad session name: {session_name}"
        finally:
            sys.argv = original

    def test_log_path_in_tmux_command(self, tmp_path):
        """The tee'd log path is under data/{review_name}/logs/."""
        original = sys.argv.copy()
        sys.argv = ["script.py", "--background"]
        try:
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop("TMUX", None)
                mock_run = MagicMock()
                with patch("shutil.which", return_value="/usr/bin/tmux"):
                    with patch("subprocess.run", mock_run):
                        with pytest.raises(SystemExit):
                            maybe_background("pipeline", review_name="my_review")

            # The bash -c command string is the last arg
            tmux_cmd = mock_run.call_args[0][0][-1]
            assert "data/my_review/logs/ee_pipeline_" in tmux_cmd
            assert "| tee" in tmux_cmd
        finally:
            sys.argv = original

    def test_background_flag_not_in_relaunched_command(self, tmp_path):
        """The re-launched command must not contain --background (prevents loops)."""
        original = sys.argv.copy()
        sys.argv = ["script.py", "--background", "--spec", "foo.yaml"]
        try:
            with patch.dict(os.environ, {}, clear=True):
                os.environ.pop("TMUX", None)
                mock_run = MagicMock()
                with patch("shutil.which", return_value="/usr/bin/tmux"):
                    with patch("subprocess.run", mock_run):
                        with pytest.raises(SystemExit):
                            maybe_background("test")

            tmux_cmd = mock_run.call_args[0][0][-1]
            assert "--background" not in tmux_cmd
        finally:
            sys.argv = original

    def test_foreground_when_no_flag(self):
        """Without --background, sys.argv is untouched."""
        original = sys.argv.copy()
        expected = ["script.py", "--spec", "foo.yaml", "--name", "bar"]
        sys.argv = expected.copy()
        try:
            maybe_background("test")
            assert sys.argv == expected
        finally:
            sys.argv = original
