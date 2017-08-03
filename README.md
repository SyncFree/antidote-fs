# Antidote file system

[![Build Status](https://travis-ci.org/SyncFree/antidote-fs.svg?branch=master)](https://travis-ci.org/SyncFree/antidote-fs)  

A file system backed by [Antidote](http://syncfree.github.io/antidote/).  
**WARNING: work in progress, alpha quality.**

## Getting started

To compile:

    ./gradlew build

Assuming an Antidote instance is reachable at `127.0.0.1:8087`,
to mount the file system under `/tmp/mnt` on Linux just issue:

    ./gradlew run -Dexec.args="-d /tmp/mnt -a 127.0.0.1:8087"

## Credits

[RainbowFS](http://rainbowfs.lip6.fr/) research project.
