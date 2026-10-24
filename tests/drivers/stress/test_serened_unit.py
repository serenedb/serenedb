import os
import subprocess

import pytest
from serened import listens_on


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="no /proc to read sockets from")
def test_a_server_that_exited_before_it_was_reaped_does_not_listen():
    proc = subprocess.Popen(["true"])
    os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT)
    try:
        assert listens_on(proc.pid, [1]) is False
    finally:
        proc.wait()
