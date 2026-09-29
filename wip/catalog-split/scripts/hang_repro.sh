#!/bin/bash
S=$1
cd /home/mironov/projects/serenedb/serenedb
(lldb --batch -o "process handle -s false -n false -p true SIGPIPE SIGUSR1 SIGUSR2 SIGCHLD SIGALRM SIGPROF" -o run -o "thread backtrace all" -o kill -- $PWD/build_clangd/bin/serened $S/hang_data --listen=postgres://0.0.0.0:6195 > $S/hang_bt2.txt 2>&1 &)
for i in $(seq 1 120); do psql -h 127.0.0.1 -p 6195 -U postgres -d postgres -Atc 'select 1' >/dev/null 2>&1 && break; sleep 0.5; done
q() { timeout ${2:-20} psql -h 127.0.0.1 -p 6195 -U postgres -d postgres -Atc "$1" 2>&1 | tail -1; }
q "CREATE DATABASE seq_other"
q "CREATE SEQUENCE seq_local"
q "CREATE SEQUENCE seq_other.public.seq_remote"
echo "2pc commit rc:"; timeout 20 psql -h 127.0.0.1 -p 6195 -U postgres -d postgres -At -c "BEGIN" -c "SELECT nextval('seq_local')" -c "SELECT nextval('seq_other.public.seq_remote')" -c "COMMIT" 2>&1 | tail -2; echo "rc=$?"
PID=$(lsof -t -i:6195 -sTCP:LISTEN | head -1)
echo "serened pid $PID"
kill -STOP $PID
for i in $(seq 1 60); do grep -q 'thread #' $S/hang_bt2.txt 2>/dev/null && break; sleep 1; done
sleep 3
kill -9 $PID 2>/dev/null
