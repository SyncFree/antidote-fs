# Antidote file system

[![Build Status](https://travis-ci.org/SyncFree/antidote-fs.svg?branch=master)](https://travis-ci.org/SyncFree/antidote-fs)  

A file system backed by [Antidote](http://syncfree.github.io/antidote/).  
**WARNING: work in progress, alpha quality.**

## Getting started

To compile:

    make 

Assuming an Antidote instance is reachable at `127.0.0.1:8087`,
to mount the file system under `/tmp/mnt` on Linux just issue:

    ./gradlew run -Dexec.args="-d /tmp/mnt -a 127.0.0.1:8087"

Some convenient make targets are available:

    # spawn an Antidote Docker container and mount Antidote-fs on ./d1
    make run

    # start and stop a local Antidote container
    make start-antidote-docker
    make stop-antidote-docker

    # ./gradlew run -Dexec.args="-d /d1 -a 127.0.0.1:8087"
    make mount-fs

## Credits

[RainbowFS](http://rainbowfs.lip6.fr/) research project.
