#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../server/
    go build ../client/
    go build ../cmd/
    rm -r logs
    mkdir logs/
    ./server -log_dir=logs -log_level=debug -id 1.1 >logs/out1.1.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 1.2 >logs/out1.2.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 1.3 >logs/out1.3.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 2.1 >logs/out2.1.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 2.2 >logs/out2.2.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 2.3 >logs/out2.3.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 3.1 >logs/out3.1.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 3.2 >logs/out3.2.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 3.3 >logs/out3.3.txt 2>&1 &
    echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
