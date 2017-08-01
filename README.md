# Antidote file system

A file system backed by [Antidote](http://syncfree.github.io/antidote/).  
Work in progress.

## Getting started

To compile:

    ./gradlew build

Supposing an Antidote instance is accessible at `127.0.0.1:8087`,
for instance by issuing the following Docker command:

    docker run --rm -it -p "8087:8087" antidotedb/antidote

to mount the file system under `/tmp/mnt` on Linux:

    ./gradlew run

