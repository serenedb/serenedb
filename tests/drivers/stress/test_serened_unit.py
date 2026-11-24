import os
import subprocess

import pytest
from serened import Serened, listens_on


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="no /proc to read sockets from")
def test_a_server_that_exited_before_it_was_reaped_does_not_listen():
    proc = subprocess.Popen(["true"])
    os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT)
    try:
        assert listens_on(proc.pid, [1]) is False
    finally:
        proc.wait()


def test_stop_cleanly_reports_an_unclean_exit(tmp_path):
    server = Serened("serened", datadir_root=str(tmp_path))
    server.proc = subprocess.Popen(["sh", "-c", "exit 3"])
    server.proc.wait()
    with pytest.raises(AssertionError, match="serened exited 3"):
        server.stop_cleanly()


def test_stop_cleanly_accepts_a_server_stopped_by_sigterm(tmp_path):
    server = Serened("serened", datadir_root=str(tmp_path))
    server.proc = subprocess.Popen(["sleep", "60"])
    server.stop_cleanly()
