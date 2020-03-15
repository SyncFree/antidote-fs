#!/bin/bash

start_single_instance() {
    echo "Starting single instance"
    docker rm -fv $(docker ps -q -f ancestor=antidotedb/antidote) 2> /dev/null
    docker run -d --rm -it -p "8087:8087" antidotedb/antidote:latest > /dev/null
    sleep 20
    rm -rf d1; mkdir d1
    node ./src/antidote-fs.js -m d1 -a "localhost:8087" > /dev/null &
    sleep 5
    echo "Single instance started"
}

stop_single_instance() {
    echo "Stopping single instance"
    fusermount -u ./d1
    docker rm -fv $(docker ps -q -f ancestor=antidotedb/antidote) > /dev/null 2>&1
    echo "Single instance stopped"
}

# Sequential test
start_single_instance
./test/fs_basic_test.sh
STATUS=$?
stop_single_instance

# Distributed test
./test/fs_distributed_test.sh
STATUS=$(( $? || $STATUS ))

exit $STATUS
