#!/bin/bash

start_single_instance() { 
    docker rm -fv $(docker ps -q -f ancestor=antidotedb/antidote) 2> /dev/null 
    docker run -d --rm -it -p "8087:8087" antidotedb/antidote > /dev/null
    sleep 5
    rm -rf d1; mkdir d1
    node ./src/antidote-fs.js -m d1 -a "localhost:8087" > /dev/null &
    sleep 2
}

stop_single_instance() {
    fusermount -u ./d1
    docker rm -fv $(docker ps -a -q) > /dev/null 2>&1
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
