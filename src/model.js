'use strict';

const defaultFileMode = 0o100777; // 33279
const defaultDirMode = 0o40777; // 16895

const defaultDirSize = 4096;

function getUnixTime() {
    return Math.floor(new Date().getTime() / 1000);
}

class Attr {
    constructor(inode, size, nlink) {
        let now = getUnixTime();

        this.inode = inode;
        this.mode = null;
        this.ctime = now;
        this.mtime = now;
        this.atime = now;

        this.rdev = 0;
        this.size = size;
        this.nlink = nlink;

        this.uid = 1000;
        this.gid = 1000;

        this.children = {};
        this.hlinks = {};
    }
    addHardLinkRef(pino, name) {
        this.hlinks[pino] = name;
    }
};

class AttrFile extends Attr {
    constructor(inode, size, nlink, mode) {
        super(inode, size, nlink);
        this.mode = mode ? mode : defaultFileMode;
        this.isFile = true;
    }
}

class AttrDir extends Attr {
    constructor(inode, nlink, mode) {
        super(inode, defaultDirSize, nlink);
        this.mode = mode ? mode : defaultDirMode;
        this.isFile = false;
    }
    addChild(name, inode) {
        this.children[name] = inode;
    }
}

module.exports = {AttrFile, AttrDir};
