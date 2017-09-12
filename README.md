# Antidote file system

[![Build Status](https://travis-ci.org/SyncFree/antidote-fs.svg?branch=master)](https://travis-ci.org/SyncFree/antidote-fs)  

A [FUSE][fuse-wiki] file system backed by [Antidote][antidote].  
**WARNING: work in progress, alpha quality.**


## Getting started

Requirements: [JDK 8][jdk8], [Antidote][antidote-setup], [Fuse 2.9][fuse] (and [Docker][docker] for the tests).  
To compile: `make` or `./gradlew build`.  

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

[RainbowFS][rainbowfs] research project.

 [antidote]: http://syncfree.github.io/antidote/
 [antidote-setup]: http://syncfree.github.io/antidote/setup.html
 [docker]: https://www.docker.com/get-docker
 [fuse]: https://github.com/libfuse/libfuse
 [fuse-wiki]: https://en.wikipedia.org/wiki/Filesystem_in_Userspace
 [jdk8]: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html 
 [rainbowfs]: http://rainbowfs.lip6.fr/
