#!/bin/bash

node ./src/antidote-fs.js > /dev/null &
sleep 2
./test/fs_basic_test.sh
STATUS=$?
fusermount -u ./d1
exit $STATUS
