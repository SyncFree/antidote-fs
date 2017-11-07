#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

rnd_str() { 
    echo $(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 5 | head -n 1)
}

ok() { echo -e "${GREEN}OK${NC}"; }
ko() { echo -e "${RED}KO${NC}"; }
