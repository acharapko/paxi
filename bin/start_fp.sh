#!/usr/bin/env bash

PID_FILE=server.pid

./server -log_dir=logs -log_level=info -id $1 -algorithm=fastpaxos -clientslots=$2 > $1.out 2>logs/$1.err &
echo $! >> ${PID_FILE}
