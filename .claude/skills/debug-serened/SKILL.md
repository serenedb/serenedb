---
name: debug-serened
description: Diagnose a crashed, hung, wedged or non-starting serened - lldb backtraces (attach or launch under lldb, depending on ptrace_scope), boot crash loops, recovery worker logs, symbolizing addresses, CI artifacts. Use when serened crashes, hangs, spins, won't boot, or tests die with Connection refused.
---

# Debugging serened

Tools: lldb (`/usr/local/bin/lldb` on the dev boxes). gdb, eu-stack and py-spy are usually not installed.

## Is it wedged, and how

1. `timeout 5 psql -h 127.0.0.1 -p <port> -U postgres -c 'SELECT 1'`: no answer means wedged.
2. Spinning or blocked: `ps -p <pid> -o %cpu,nlwp`. `absl::Mutex` spins before it sleeps, so lock contention can look like a busy loop.
3. Take the backtrace BEFORE killing anything; the state is gone afterwards. Kill only your own pid.

## Backtraces

`kernel.yama.ptrace_scope` differs between machines (`cat /proc/sys/kernel/yama/ptrace_scope`):

- 0: attach works.

  ```bash
  lldb -p <pid> --batch -o "thread backtrace all" -o detach -o quit > /tmp/$USER-bt.txt
  ```

- 1: attaching to a process that isn't lldb's child fails. Launch serened under lldb instead, driven through a FIFO:

  ```bash
  SC=$(mktemp -d /tmp/$USER-lldb.XXXX)
  mkfifo $SC/lldbin
  ( tail -f $SC/lldbin ) | lldb --no-lldbinit > $SC/lldb.log 2>&1 &
  LLDB_PID=$!
  { echo "settings set auto-confirm true"
    echo "target create ./build/bin/serened"
    echo "process launch -- ./build_data_x --listen=postgres://0.0.0.0:<port>"; } > $SC/lldbin
  # when it wedges: lldb doesn't read stdin while the inferior runs, so interrupt first
  kill -INT $LLDB_PID; echo "thread backtrace all" > $SC/lldbin
  ```

The debug binary is several GB; attach to one process at a time. Group a deadlock's threads by the `absl::Mutex` address they block on.

## Crash at boot (restart loop)

A datadir that makes serened exit at startup loops forever under `run_serened_loop.sh` and shows up as "Connection refused" in tests. Reproduce on a plain serened with the kept datadir, then:

```bash
lldb --batch -o "breakpoint set -r 'InternalException::InternalException'" \
  -o run -o "bt 40" -- ./build/bin/serened <datadir> --listen=postgres://0.0.0.0:<port>
```

After changes to catalog create/alter paths or persisted formats, always restart once on a kept datadir after a `CHECKPOINT`: the fast suite starts on a fresh datadir and never runs the boot replay paths.

## Logs and symbols

- Recovery runs keep per-test server logs in `/tmp/serened-logs-XXXXXX/worker-N-test-M.log`.
- Split DWARF keeps debug info in `.dwo` files beside the objects: symbolize from the build directory that produced the binary. `llvm-symbolizer --obj=<binary> <addr>`, or `addr2line -e <binary> -f <addr>`.
- CI: the job log is `gh api repos/serenedb/serenedb/actions/jobs/<job-id>/logs`; sanitizer reports, service logs and the sqllogic log are in the run's `test-artifacts-<config>-linux-amd64-<run number>` artifact (`sanitizers/`, `logs/`). See the `investigate-ci` skill.

## Ground rules

- Diagnose from the stack, the code and the commits that wrote it (what changed against main, upstream or our patch). Build or run ASAN/TSAN/MSAN locally only when the user asks.
- An OOM kill inside a tmux pane stops the whole pane, Claude included: run suspect reproductions in their own scope (`systemd-run --user --scope -p MemoryMax=64G ...`) or under `ulimit -v`.
