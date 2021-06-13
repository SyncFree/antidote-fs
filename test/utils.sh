#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

rnd_str() {
    echo $(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 5 | head -n 1)
}

ok() { echo -e "${GREEN}OK${NC}"; }
ko() { echo -e "${RED}KO${NC}"; }

wait_antidote() {
    while ! docker exec -it $1 grep "Application antidote started" /opt/antidote/log/console.log; do
      echo "Waiting 2 seconds for $1 to start up..."
      sleep 2
    done
}

stop_remove_antidote_containers() {
    # WARNING: this command stops and removes all containers derived by
    # antidotedb/antidote!
    docker rm -fv $(docker ps -q -f ancestor=antidotedb/antidote) 2> /dev/null
}

