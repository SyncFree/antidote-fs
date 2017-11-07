#!/bin/bash
# Script to test AntidoteFS in a simple distributed setting.

. ./test/utils.sh

docker-compose -f ./test/docker-antidote-3dcs.yml down >/dev/null 2>&1
docker-compose -f ./test/docker-antidote-3dcs.yml up -d >/dev/null 2>&1
sleep 8

rm -rf d1 d2 d3
mkdir -p d1 d2 d3
node ./src/antidote-fs.js -m d1 -a "localhost:8087" > /dev/null &
node ./src/antidote-fs.js -m d2 -a "localhost:8088" > /dev/null &
node ./src/antidote-fs.js -m d3 -a "localhost:8089" > /dev/null &
sleep 3

EXIT=0

# File naming conflict: rename
echo hello there 1 > ./d1/test.txt &
echo hello there 2 > ./d2/test.txt
sleep 2
echo -n "File conflict.................."
if [[ -f ./d3/test.txt-CONFLICT_0 && -f ./d3/test.txt-CONFLICT_1 ]]
then ok; 
else ko; EXIT=1; 
fi

# Directory naming conflict: merge directories
mkdir -p ./d1/dirA
echo "hello world A" > ./d1/dirA/mydirAfile.txt
mkdir ./d1/dirA/dirAA
mkdir -p ./d2/dirB/dirBB
echo "hello world B" > ./d2/dirB/dirBB/mydirBBfile.txt
mv ./d1/dirA/ ./d1/dirC/ & mv ./d2/dirB/ ./d2/dirC/
sleep 2
echo -n "Directory conflict............."
if [[ -d ./d3/dirC && \
    -d ./d3/dirC/dirAA  &&
    -d ./d3/dirC/dirBB &&
    -f ./d3/dirC/dirBB/mydirBBfile.txt &&
    -f ./d3/dirC/mydirAfile.txt &&
    $(< ./d3/dirC/dirBB/mydirBBfile.txt) == $(echo "hello world B") &&
    $(< ./d1/dirC/mydirAfile.txt) == $(echo "hello world A") ]]
then ok; 
else ko; EXIT=1;
fi

fusermount -u d1; fusermount -u d2; fusermount -u d3
killall node >/dev/null 2>&1 # !!
docker-compose -f ./test/docker-antidote-3dcs.yml down >/dev/null 2>&1
rm -rf d1 d2 d3

exit $EXIT
