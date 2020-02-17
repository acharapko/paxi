#!/usr/bin/env bash

PID_FILE=server.pid
echo "ffp $1 -p1q=$2 -p2qf=$3 -p2qc=$4 -clientslots=$5"
./server -log_dir=logs -log_level=info -id $1 -algorithm=fastpaxos -p1q=$2 -p2qf=$3 -p2qc=$4 -clientslots=$5 > $1.out 2>logs/$1.err &
echo $! >> ${PID_FILE}
