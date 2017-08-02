# Antidote file system

[![Build Status](https://travis-ci.org/SyncFree/antidote-fs.svg?branch=master)](https://travis-ci.org/SyncFree/antidote-fs)  

A file system backed by [Antidote](http://syncfree.github.io/antidote/).  
**WARNING: Work in progress, alpha quality.**

## Getting started

To compile:

    ./gradlew build

Supposing an Antidote instance is accessible at `127.0.0.1:8087`,
for instance by issuing the following Docker command:

    docker run --rm -it -p "8087:8087" antidotedb/antidote

to mount the file system under `/tmp/mnt` on Linux just issue:

    ./gradlew run

## Credits

[RainbowFS project](http://rainbowfs.lip6.fr/).
