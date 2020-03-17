#!/bin/bash

. ./test/utils.sh

start_single_instance() {
    stop_remove_antidote_containers
    echo "Starting single instance"
    docker run -d --name antidote --rm -it -p "8087:8087" antidotedb/antidote:latest > /dev/null
    wait_antidote antidote

    rm -rf d1; mkdir d1
    node ./src/antidote-fs.js -m d1 -a "localhost:8087" > /dev/null &
    sleep 5
    echo "Single instance started"
}

stop_single_instance() {
    echo "Stopping single instance"
    fusermount -u ./d1
    stop_remove_antidote_containers
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
