'use strict';

const antidoteClient = require('antidote_ts_client');
const exec = require('child_process').exec;

const fuse = require('fusejs').fuse;
const PosixError = require('fusejs').PosixError;
const FileSystem = require('fusejs').FileSystem;

const AttrFile = require('./model').AttrFile;
const AttrDir = require('./model').AttrDir;

const nodejsAssert = require('assert');

const DEBUG = true;
function log(...args) {
    if (DEBUG) {
        console.log(...args);
    }
}
function assert(...args) {
    if (DEBUG) {
        nodejsAssert(...args);
    }
}

const TIMEOUT = 3600;

// Inode generation
const INODE_HIGH = Math.pow(2, 20);
const INODE_LOW = 1000;
// Create a random inode number.
function getRandomIno() {
    // In production, use UUID or include unique per-client or per-site prefix
    return Math.floor(Math.random() * (INODE_HIGH - INODE_LOW) + INODE_LOW);
}

// File modes
const S_IFREG = 0x8000; // regular file
const S_IFDIR = 0x4000; // directory
const S_IFLNK = 0xA000; // symbolic link
// const S_IFMT = 0xF000; // bit mask for the file type bit field
// const S_IFSOCK = 0xC000; // socket
// const S_IFBLK = 0x6000; // block device
// const S_IFCHR = 0x2000; // character device
// const S_IFIFO = 0x1000; // FIFO

/**
 * Not implemented Fuse operations:
 * forget, multiforget, getlk, setlk, setxattr
 * getxattr, listxattr, removexattr, bmap, ioctl, poll.
 */
class AntidoteFS extends FileSystem {
    /**
     * Initialize the file system.
     * Called before any other file system method.
     *
     * @param {Object} connInfo Fuse connection information.
     *
     * (There's no reply to this function.)
     **/
    async init(connInfo) {
        log('INIT: connInfo', JSON.stringify(connInfo));
        const rootAttr = await antidote.register('inode_1').read();
        if (!rootAttr || DEBUG) {
            if (DEBUG) {
                // Populate file system with a few dummy files and dirs
                const root = new AttrDir(1, 2, null);
                root.addChild('file.txt', 2);
                root.addChild('folder', 3);

                const fileContent = 'Hello world';
                const f = new AttrFile(2, fileContent.length, 1, null);
                f.addHardLinkRef(1, 'file.txt');

                const d = new AttrDir(3, 2, null);
                d.addHardLinkRef(1, 'folder');

                await antidote.update(
                    [antidote.register(`inode_${root.inode}`).set(root),
                    antidote.register(`inode_${f.inode}`).set(f),
                    antidote.register(`inode_${d.inode}`).set(d),
                    antidote.register('data_2').set(new Buffer(fileContent))]
                );
                log('Finished populating file system.');
                return;
            } else {
                // Not in debug, but didn't find root inode: create one
                const root = new AttrDir(1, 2, null);
                await antidote.update(
                    antidote.register(`inode_${root.inode}`).set(root));
                log('Created new root directory.');
                return;
            }
        }
    }


    /**
     * Clean up. Called on file system exit (upon umount).
     * (There's no reply to this function.)
     **/
    async destroy() {
        log('DESTROY');
        antidote.close();
    }

    /**
     * Look up a directory entry by name and get its attributes.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} pino Inode number of the parent directory.
     * @param {String} name the name to look up.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.entry() or reply.err().
     **/
    async lookup(context, pino, name, reply) {
        log('LOOKUP: pino', pino, 'name', name);
        let attr = await antidote.register(`inode_${pino}`).read();
        if (attr && attr.children[name]) {
            let childAttr = await
                antidote.register(`inode_${attr.children[name]}`).read();
            if (childAttr) {
                log('lookup replying: ', childAttr.inode);
                const entry = {
                    inode: childAttr.inode,
                    attr: childAttr,
                    generation: 1,
                };
                reply.entry(entry);
                return;
            } else {
                reply.err(PosixError.ENOENT);
                return;
            }
        } else {
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Get file attributes.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode Inode number.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.attr() or reply.err()
     **/
    async getattr(context, inode, reply) {
        log('GETATTR: ', inode);
        let attr = await antidote.register(`inode_${inode}`).read();
        if (attr) {
            log('getattr replying: ', JSON.stringify(attr));
            reply.attr(attr, TIMEOUT);
            return;
        } else {
            reply.err(PosixError.ENOTENT);
            return;
        }
    }

    /**
     * Set file attributes.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode Inode Number.
     * @param {Object} attrs Attributes to be set.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.attr() or reply.err();
     **/
    async setattr(context, inode, attr, reply) {
        log('SETATTR inode', inode, 'attr', JSON.stringify(attr));
        let iattr = await antidote.register(`inode_${inode}`).read();

        const keys = Object.keys(attr);
        for (let i = 0; i < keys.length; i++) {
            log('updating attribute', keys[i], 'to', attr[keys[i]]);
            if (keys[i] == 'atime' || keys[i] == 'mtime') {
                if (attr[keys[i]] == -1) {
                    iattr[keys[i]] = Math.floor(new Date().getTime() / 1000);
                } else {
                    iattr[keys[i]] = Math.floor(new Date(attr[keys[i]]).getTime() / 1000);
                }
            } else {
                iattr[keys[i]] = attr[keys[i]];
            }
        }
        // log('setattr set inode ', inode, 'iattr', iattr );
        await antidote.update(antidote.register(`inode_${inode}`).set(iattr));
        reply.attr(iattr, TIMEOUT);
    }

    /**
     * Create file node.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} parent Inode number of the parent directory.
     * @param {String} name Name to be created.
     * @param {Number} mode File type and mode with which
     * to create the new file.
     * @param {Number} rdev The device number
     * (only valid if created file is a device).
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.entry() or reply.err()
     **/
    async mknod(context, parent, name, mode, rdev, reply) {
        log('MKNOD pino', parent, 'name', name, 'mode', mode, 'rdev', rdev);
        this.createentry(parent, name, mode, rdev, false, reply);
    }

    /**
     * Create a directory.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} parent Inode number of the parent directory.
     * @param {String} name Name to be created.
     * @param {Number} mode with which to create the new file.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.entry() or reply.err()
     **/
    async mkdir(context, parent, name, mode, reply) {
        log('MKDIR pino', parent, 'name', name, 'mode', mode);
        this.createentry(parent, name, mode, 0, true, reply);
    }

    async createentry(pino, name, mode, rdev, isdir, reply) {
        // log('CREATEENTRY pino', pino, 'name', name, 'mode', mode, 'rdev', rdev, 'isdir', isdir);
        const inode = getRandomIno();

        let st = isdir ? new AttrDir(inode, 2, S_IFDIR | mode) :
            new AttrFile(inode, 0, 1, S_IFREG | mode);
        st.addHardLinkRef(pino, name);

        let attr = await antidote.register(`inode_${pino}`).read();
        if (attr) {
            attr.children[name] = inode;
            await antidote.update([
                antidote.register(`inode_${pino}`).set(attr),
                antidote.register(`inode_${inode}`).set(st),
            ]);
            reply.entry({ inode: inode, attr: st, generation: 1 });
            return;
        } else {
            reply.err(ERR.ENXIO);
            return;
        }
    }

    /**
     * Remove a file.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} pino Inode number of the parent directory.
     * @param {String} name Name of the file to remove.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.err()
     **/
    async unlink(context, pino, name, reply) {
        log('UNLINK pino', pino, 'name', name);
        let pattr = await antidote.register(`inode_${pino}`).read();
        if (pattr) {
            assert(!pattr.isFile);
            const ino = pattr.children[name];
            let attr = await antidote.register(`inode_${ino}`).read();
            if (attr) {
                assert(attr.isFile);
                attr.nlink--;
                delete attr.hlinks[pino];
                delete pattr.children[name];
                await antidote.update([
                    antidote.register(`inode_${pino}`).set(pattr),
                    antidote.register(`inode_${ino}`).set(
                        attr.nlink ? attr : null
                    ),
                ]);
                reply.err(0);
                return;
            } else {
                reply.err(PosixError.ENOENT);
                return;
            }
        } else {
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Remove an empty directory.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} pino Inode number of the parent directory.
     * @param {String} name Name of the directory to remove.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.err()
     **/
    async rmdir(context, pino, name, reply) {
        log('RMDIR pino', pino, 'name', name);
        let pattr = await antidote.register(`inode_${pino}`).read();
        if (pattr) {
            const ino = pattr.children[name];
            let attr = await antidote.register(`inode_${ino}`).read();
            if (attr) {
                if (!attr.isFile) {
                    if (Object.keys(attr.children).length <= 0) {
                        delete pattr.children[name];
                        await antidote.update([
                            antidote.register(`inode_${pino}`).set(pattr),
                            antidote.register(`inode_${ino}`).set(null),
                        ]);
                        reply.err(0);
                        return;
                    } else {
                        // Directory is not empty
                        reply.err(PosixError.ENOTEMPTY);
                        return;
                    }
                } else {
                    // Target inode is not a directory
                    reply.err(PosixError.ENOTDIR);
                    return;
                }
            } else {
                // Target inode does not exist
                reply.err(PosixError.ENOENT);
                return;
            }
        } else {
            // Parent inode does not exist
            reply.err(PosixError.ENXIO);
            return;
        }
    }

    /**
     * Create a symbolic link.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} pino Inode number of the parent directory.
     * @param {String} link The contents of the symbolic link.
     * @param {String} name Name of the symbolic link to create.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.entry() or reply.err()
     **/
    async symlink(context, pino, link, name, reply) {
        log('SYMLINK link', link, 'pino', pino, 'name', name);
        let pattr = await antidote.register(`inode_${pino}`).read();
        if (pattr) {
            const existingIno = pattr.children[name];
            if (!existingIno) {
                const inode = getRandomIno();
                let st = new AttrFile(inode, link.length, 1, S_IFLNK | 0x124);
                st.addHardLinkRef(pino, name);

                pattr.children[name] = inode;

                /* It writes as a simple string `link`,
                 * which is then read by `readlink`.
                 * Ex.: `ln -s myfile mylink` will write myfile
                 */
                await antidote.update(
                    [antidote.register(`inode_${pino}`).set(pattr),
                    antidote.register(`inode_${inode}`).set(st),
                    antidote.register(`data_${inode}`).set(new Buffer(link))]
                );

                const entry = {
                    inode: inode,
                    attr: st,
                    generation: 1,
                };
                reply.entry(entry);
                return;
            } else {
                // This name already exists in the directory
                reply.err(PosixError.EEXIST);
                return;
            }
        } else {
            // Parent directory does not exist
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Read symbolic link.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode Inode number.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.readlink() or reply.err()
     **/
    async readlink(context, inode, reply) {
        log('READLINK ino', inode);
        let data = await antidote.register(`data_${inode}`).read();
        if (data) {
            // log('read: ', data.toString());
            reply.readlink(data.toString());
            return;
        } else {
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Create a hard link.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode The old inode number.
     * @param {Number} newpino Inode number of the new parent directory.
     * @param {String} newname New name to create.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.entry() or reply.err()
     **/
    async link(context, inode, newpino, newname, reply) {
        log('LINK inode', inode, 'newpino', newpino, 'newname', newname);
        let pattr = await antidote.register(`inode_${newpino}`).read();
        let attr = await antidote.register(`inode_${inode}`).read();
        if (pattr && attr) {
            if (attr.isFile) {
                const existingIno = pattr.children[newname];
                if (!existingIno) {
                    pattr.children[newname] = inode;

                    attr.nlink++;
                    attr.hlinks[newpino] = newname;

                    await antidote.update([
                        antidote.register(`inode_${newpino}`).set(pattr),
                        antidote.register(`inode_${inode}`).set(attr),
                    ]);

                    const entry = {
                        inode: attr.inode,
                        attr: attr,
                        generation: 1,
                    };
                    reply.entry(entry);
                    return;
                } else {
                    // This name already exists in the directory
                    reply.err(PosixError.EEXIST);
                    return;
                }
            } else {
                // Target inode is a directory
                reply.err(PosixError.EISDIR);
                return;
            }
        } else {
            // New parent or target inode does not exist
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Rename (or move) a file.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} pino Inode number of the old parent directory.
     * @param {String} name Old name.
     * @param {Number} newpino Inode number of the new parent directory.
     * @param {String} newname New name.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.err()
     **/
    async rename(context, pino, name, newpino, newname, reply) {
        log('RENAME pino', pino, 'name', name, 'newpino', newpino,
            'newname', newname);

        let pattr = await antidote.register(`inode_${pino}`).read();
        if (pattr && pattr.children[name]) {
            assert(!pattr.isFile);
            const ino = pattr.children[name];
            let attr = await antidote.register(`inode_${ino}`).read();
            delete attr.hlinks[pino];
            attr.hlinks[newpino] = newname;
            delete pattr.children[name];

            if (pino == newpino) {
                // rename in the same directory    
                pattr.children[newname] = ino;
                await antidote.update([
                    antidote.register(`inode_${pino}`).set(pattr),
                    antidote.register(`inode_${ino}`).set(attr),
                ]);
            } else {
                // move to another directory
                let pnattr = await antidote.register(`inode_${newpino}`).read();
                assert(pnattr && !pnattr.isFile);
                pnattr.children[newname] = ino;

                await antidote.update([
                    antidote.register(`inode_${newpino}`).set(pnattr),
                    antidote.register(`inode_${pino}`).set(pattr),
                    antidote.register(`inode_${ino}`).set(attr),
                ]);
            }
            reply.err(0);
            return;
        } else {
            // Target inode does not exist
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Open a file.
     *
     * Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC)
     * are available in fileInfo.flags.
     * However, AntidoteFS implements stateless file I/O, 
     * so it does not set any flags or handles (pointers, indexes, etc.).
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode The inode number.
     * @param {Object} fileInfo File information.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.open() or reply.err()
     **/
    async open(context, inode, fileInfo, reply) {
        log('OPEN: ', inode);
        /* It should check the that inode exists and that
         * it is not a directory, but most likely this is
         * already checked in calls to other operations before.
         */
        reply.open(fileInfo);
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} fileInfo 
     * @param {*} reply 
     */
    async opendir(context, inode, fileInfo, reply) {
        log('OPENDIR: ', inode);
        reply.open(fileInfo);
    }

    /**
     * Read file data.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode The inode number.
     * @param {Number} len The number of bytes to read.
     * @param {Number} offset The offset.
     * @param {Object} fileInfo File information.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.buffer() or fuse.err()
     **/
    async read(context, inode, len, offset, fileInfo, reply) {
        log('READ inode', inode, 'len', len, 'off', offset);
        let data = await antidote.register(`data_${inode}`).read();
        if (data) {
            log('read: ', data);
            const content = data.slice(offset,
                Math.min(data.length, offset + len));
            reply.buffer(new Buffer(content), content.length);
            return;
        } else {
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} size 
     * @param {*} offset 
     * @param {*} fileInfo 
     * @param {*} reply 
     */
    async readdir(context, inode, size, offset, fileInfo, reply) {
        log('READDIR inode', inode, 'size', size, 'off', offset);
        /* 
         * fileInfo will contain the value set by the opendir method, 
         * or will be undefined if the opendir method didn't set any value.
         */
        let attr = await antidote.register(`inode_${inode}`).read();
        if (attr) {
            if (Object.keys(attr.children).length > 0) {
                for (let name in attr.children) {
                    if (attr.children.hasOwnProperty(name)) {
                        let ch = await antidote.register(`inode_${attr.children[name]}`).read();
                        if (ch) {
                            log('readdir replying: inode', ch.inode, 'name', name);
                            reply.addDirEntry(name, size, ch, offset);
                        }
                    }
                }
                // send an empty buffer at the end of the stream
                reply.buffer(new Buffer(0), size);
                return;
            } else {
                reply.err(0);
                return;
            }
        } else {
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} buf 
     * @param {*} off 
     * @param {*} fi 
     * @param {*} reply 
     */
    async write(context, inode, buf, off, fi, reply) {
        log('WRITE inode', inode, 'buf.length', buf.length, 'off', off, 'buf', buf);
        let attr = await antidote.register(`inode_${inode}`).read();
        if (attr) {
            let data = await antidote.register(`data_${inode}`).read();
            if (data != null) {
                data = data.slice(0, off) + buf +
                    (off + buf.length >= attr.size ? '' :
                        data.slice(off + buf.length, attr.size));
            } else {
                data = buf;
            }
            attr.size = data.length;
            await antidote.update([
                antidote.register(`inode_${inode}`).set(attr),
                antidote.register(`data_${inode}`).set(data),
            ]);
            reply.write(buf.length);
            return;
        } else {
            reply.err(PosixError.ENXIO);
            return;
        }
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} fi 
     * @param {*} reply 
     */
    async flush(context, inode, fi, reply) {
        log('FLUSH ino', inode);
        // TODO: ?
        reply.err(0);
    }

    /**
     * 
     */
    async fsync(context, ino, datasync, fi, reply) {
        log('FSYNC ino', ino, 'datasync', datasync);
        // TODO: ?
        reply.err(0);
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} datasync 
     * @param {*} fi 
     * @param {*} reply 
     */
    async fsyncdir(context, inode, datasync, fi, reply) {
        log('FSYNCDIR inode', inode, 'datasync', datasync);
        // TODO: ?
        reply.err(0);
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} fileInfo 
     * @param {*} reply 
     */
    async release(context, inode, fileInfo, reply) {
        log('RELEASE inode', inode);
        reply.err(0);
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} fileInfo 
     * @param {*} reply 
     */
    async releasedir(context, inode, fileInfo, reply) {
        log('RELEASEDIR: ', inode);
        reply.err(0);
    }


    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} mask 
     * @param {*} reply 
     */
    async access(context, inode, mask, reply) {
        log('ACCESS inode', inode, 'mask', mask);
        // TODO: check validity of access
        reply.err(0);
    }

    /**
     * 
     * @param {*} context 
     * @param {*} pino 
     * @param {*} name 
     * @param {*} mode 
     * @param {*} fi 
     * @param {*} reply 
     */
    async create(context, pino, name, mode, fi, reply) {
        log('CREATE pino', pino, 'name', name, 'mode', mode);
        reply.err(PosixError.ENOSYS);
    }

    /**
     * 
     * @param {*} context 
     * @param {*} inode 
     * @param {*} reply 
     */
    async statfs(context, inode, reply) {
        log('STATFS inode', inode);
        reply.statfs({
            bsize: 65536,
            iosize: 65536,
            frsize: 65536,
            blocks: 1000000,
            bfree: 1000000,
            bavail: 1000000,
            files: 1000000,
            ffree: 1000000,
            favail: 1000000,
            fsid: 1000000,
            flag: 0,
        });
    }
}

function mkTmpDir() {
    const fs = require('fs');
    const dir = './d1';
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
    return dir;
}

const mountPoint = process.argv.length > 2 ?
    process.argv.slice(2, process.argv.length) : mkTmpDir();
let antidote = antidoteClient.connect(8087, 'localhost');

fuse.mount({
    filesystem: AntidoteFS,
    options: ['AntidoteFS', mountPoint],
});

function unmount() {
    log('Close antidote connection and unmount fs.');
    antidote.close();
    exec('fusermount -u ' + mountPoint);
}
process.on('SIGINT', unmount);
