'use strict';

const antidoteClient = require('antidote_ts_client');
const exec = require('child_process').exec;

const fuse = require('fusejs').fuse;
const PosixError = require('fusejs').PosixError;
const FileSystem = require('fusejs').FileSystem;

const AttrFile = require('./model').AttrFile;
const AttrDir = require('./model').AttrDir;
const getUnixTime = require('./model').getUnixTime;

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
function isEmptyObject(obj) {
    return !Object.keys(obj).length;
}

// Fuse inode attributes' validity timeout
const TIMEOUT = 3600;

// File modes
const S_IFREG = 0x8000; // regular file
const S_IFDIR = 0x4000; // directory
const S_IFLNK = 0xA000; // symbolic link

// Inode generation
const INODE_HIGH = Math.pow(2, 20);
const INODE_LOW = 1000;


/**
 * AntidoteFS main class.
 * Extends fusejs.FileSystem and includes
 * functions to exchange data with AntidoteDB.
 * 
 * NB: not implemented Fuse operations:
 * flush, fsync, fsyncdir, release, access,
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
        let root = await this.readMd(1);
        if (isEmptyObject(root) || DEBUG) {
            if (DEBUG) {
                // Populate file system with a few dummy files and dirs
                root = new AttrDir(1, 2, null);
                root.addChild('file.txt', 2);
                root.addChild('dirA', 3);

                const dummyFileContent = 'Hello world';
                const dummyFile = new AttrFile(2, dummyFileContent.length, 1, null);
                dummyFile.addHardLinkRef(1, 'file.txt');

                const dummyDir = new AttrDir(3, 2, null);
                dummyDir.addHardLinkRef(1, 'dirA');

                await antidote.update(
                    Array.prototype.concat(
                        this.mdUpdate(root),
                        this.mdUpdate(dummyFile),
                        this.mdUpdate(dummyDir),
                        this.dataUpdate(dummyFile, dummyFileContent),
                    )
                );
                log('Finished populating file system.');
                return;
            } else {
                // Not in debug, but didn't find root inode: create one
                root = new AttrDir(1, 2, null);
                await antidote.update(this.mdUpdate(root));
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
        let attr = await this.readMd(pino);
        if (!isEmptyObject(attr) && attr.children[name]) {
            let childAttr = await this.readMd(attr.children[name]);
            if (!isEmptyObject(childAttr)) {
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
        let attr = await this.readMd(inode);
        if (!isEmptyObject(attr)) {
            log('getattr replying: ', JSON.stringify(attr));
            reply.attr(attr, TIMEOUT);
            return;
        } else {
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Set file attributes.
     *
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode Inode Number.
     * @param {Object} attr Attributes to be set.
     * @param {Object} reply Reply instance.
     *
     * Valid replies: reply.attr() or reply.err();
     **/
    async setattr(context, inode, attr, reply) {
        log('SETATTR inode', inode, 'attr', JSON.stringify(attr));
        let iattr = await this.readMd(inode);

        const keys = Object.keys(attr);
        for (let i = 0; i < keys.length; i++) {
            log('updating attribute', keys[i], 'to', attr[keys[i]]);
            if (keys[i] == 'atime' || keys[i] == 'mtime') {
                if (attr[keys[i]] == -1) {
                    iattr[keys[i]] = getUnixTime();
                } else {
                    iattr[keys[i]] = getUnixTime(attr[keys[i]]);
                }
            } else {
                iattr[keys[i]] = attr[keys[i]];
            }
        }
        await antidote.update(this.mdUpdate(iattr));
        reply.attr(iattr, TIMEOUT);
        return;
    }

    /**
     * Create a file.
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
        this.createEntry(parent, name, mode, rdev, false, reply);
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
        this.createEntry(parent, name, mode, 0, true, reply);
    }

    async createEntry(pino, name, mode, rdev, isdir, reply) {
        // log('CREATEENTRY pino', pino, 'name', name, 'mode', mode, 'rdev', rdev, 'isdir', isdir);
        const inode = this.getRandomIno();
        let attr = isdir ? new AttrDir(inode, 2, S_IFDIR | mode) :
            new AttrFile(inode, 0, 1, S_IFREG | mode);
        attr.addHardLinkRef(pino, name);

        let pattr = await this.readMd(pino);
        if (!isEmptyObject(pattr)) {
            if (!pattr.children[name]) {
                pattr.children[name] = inode;
                await antidote.update(
                    Array.prototype.concat(
                        this.mdUpdate(pattr),
                        this.mdUpdate(attr),
                    )
                );
                reply.entry({ inode: inode, attr: attr, generation: 1 });
                return;
            } else {
                // This name already exists in the directory
                reply.err(PosixError.EEXIST)
                return;
            }
        } else {
            // Parent inode does not exist
            reply.err(ERR.ENXIO);
            return;
        }
    }

    /**
     * Remove a (hard link of a) file.
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
        let pattr = await this.readMd(pino);
        if (!isEmptyObject(pattr)) {
            assert(!pattr.isFile);
            const ino = pattr.children[name];
            let attr = await this.readMd(ino);
            if (!isEmptyObject(attr)) {
                assert(attr.isFile);
                attr.nlink--;
                delete attr.hlinks[pino];
                delete pattr.children[name];
                log('unlink: ', JSON.stringify(pattr), JSON.stringify(attr));
                await antidote.update(
                    Array.prototype.concat(
                        this.mdDeleteChild(pattr, name),
                        (attr.nlink ?
                            this.mdDeleteHlink(attr, pino) :
                            this.mdDelete(attr)),
                    )
                );
                reply.err(0);
                return;
            } else {
                // Target inode does not exist
                reply.err(PosixError.ENOENT);
                return;
            }
        } else {
            // Parent inode does not exist
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
        let pattr = await this.readMd(pino);
        if (!isEmptyObject(pattr)) {
            assert(!pattr.isFile);
            const ino = pattr.children[name];
            let attr = await this.readMd(ino);
            if (!isEmptyObject(attr)) {
                if (!attr.isFile) {
                    if (Object.keys(attr.children).length <= 0) {
                        delete pattr.children[name];
                        await antidote.update(
                            Array.prototype.concat(
                                this.mdDeleteChild(pattr, name),
                                this.mdDelete(attr)
                            )
                        );
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
        let pattr = await this.readMd(pino);
        if (!isEmptyObject(pattr)) {
            const existingIno = pattr.children[name];
            if (!existingIno) {
                const inode = this.getRandomIno();
                let st = new AttrFile(inode, link.length, 1, S_IFLNK | 0x124);
                st.addHardLinkRef(pino, name);

                pattr.children[name] = inode;

                /* It writes as a simple string `link`,
                 * which is then read by `readlink`.
                 * Ex.: `ln -s myfile mylink` will write myfile
                 */
                await antidote.update(
                    Array.prototype.concat(
                        this.mdUpdate(pattr),
                        this.mdUpdate(st),
                        this.dataUpdate(st, link)
                    )
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
        let data = await this.readData(inode);
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
        let pattr = await this.readMd(newpino);
        let attr = await this.readMd(inode);
        if (!isEmptyObject(pattr) && !isEmptyObject(attr)) {
            if (attr.isFile) {
                const existingIno = pattr.children[newname];
                if (!existingIno) {
                    pattr.children[newname] = inode;

                    attr.nlink++;
                    attr.hlinks[newpino] = newname;

                    await antidote.update(
                        Array.prototype.concat(
                            this.mdUpdate(pattr),
                            this.mdUpdate(attr),
                        )
                    );

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

        let pattr = await this.readMd(pino);
        if (!isEmptyObject(pattr) && pattr.children[name]) {
            assert(!pattr.isFile);
            const ino = pattr.children[name];
            let attr = await this.readMd(ino);
            delete attr.hlinks[pino];
            attr.hlinks[newpino] = newname;
            delete pattr.children[name];

            if (pino == newpino) {
                // rename in the same directory    
                pattr.children[newname] = ino;
                log('rename same dir, writing pattr', JSON.stringify(pattr));
                log('rename same dir, writing attr', JSON.stringify(attr));
                await antidote.update(
                    Array.prototype.concat(
                        this.mdDeleteChild(pattr, name),
                        // overwriting: delete references to inodes to the same name
                        this.mdDeleteChild(pattr, newname),
                        this.mdDeleteHlink(attr, pino),
                        this.mdUpdate(attr),
                        this.mdUpdate(pattr)
                    )
                );
            } else {
                // move to another directory
                let pnattr = await this.readMd(newpino);
                assert(!isEmptyObject(pnattr) && !pnattr.isFile);
                pnattr.children[newname] = ino;

                await antidote.update(
                    Array.prototype.concat(
                        this.mdDeleteChild(pattr, name),
                        // overwriting: delete references to inodes to the same name
                        this.mdDeleteChild(pnattr, newname),
                        this.mdDeleteHlink(attr, pino),
                        this.mdUpdate(attr),
                        this.mdUpdate(pnattr)
                    )
                );
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
     * Open a directory.
     * 
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode The inode number.
     * @param {Object} fileInfo File information. 
     * @param {Object} reply Reply instance.
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
        let data = await this.readData(inode);
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
     * Read a directory.
     * 
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode The inode number.
     * @param {Number} size The directory size.
     * @param {Number} offset The offset.
     * @param {Object} fileInfo File information.
     * @param {Object} reply Reply instance.
     */
    async readdir(context, inode, size, offset, fileInfo, reply) {
        log('READDIR inode', inode, 'size', size, 'off', offset);
        /* 
         * fileInfo will contain the value set by the opendir method, 
         * or will be undefined if the opendir method didn't set any value.
         */
        let attr = await this.readMd(inode);
        if (!isEmptyObject(attr)) {
            if (Object.keys(attr.children).length > 0) {
                for (let name in attr.children) {
                    if (attr.children.hasOwnProperty(name)) {
                        let ch = await this.readMd(attr.children[name]);
                        if (!isEmptyObject(ch)) {
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
            // Target inode does not exist
            reply.err(PosixError.ENOENT);
            return;
        }
    }

    /**
     * Write file data.
     * 
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode The inode number.
     * @param {Object} buf Buffer of data to be written.
     * @param {Number} offset The offset.
     * @param {Object} fileInfo File information.
     * @param {Object} reply Reply instance.
     */
    async write(context, inode, buf, offset, fileInfo, reply) {
        log('WRITE inode', inode, 'buf.length', buf.length, 'off', offset, 'buf', buf);
        let attr = await this.readMd(inode);
        if (!isEmptyObject(attr)) {
            let data = await this.readData(inode);
            if (data != null) {
                data = data.slice(0, offset) + buf +
                    (offset + buf.length >= attr.size ? '' :
                        data.slice(offset + buf.length, attr.size));
            } else {
                data = buf;
            }
            attr.size = data.length;
            await antidote.update(
                Array.prototype.concat(
                    /* TODO: add back on every parent inode children map
                     * this inode so that in case of concurrent delete, 
                     * the add will prevail.
                     */
                    this.mdUpdate(attr),
                    this.dataUpdate(attr, data),
                )
            );
            reply.write(buf.length);
            return;
        } else {
            reply.err(PosixError.ENXIO);
            return;
        }
    }

    async releasedir(context, inode, fileInfo, reply) {
        log('RELEASEDIR: ', inode);
        reply.err(0);
    }

    async create(context, pino, name, mode, fi, reply) {
        log('CREATE pino', pino, 'name', name, 'mode', mode);
        reply.err(PosixError.ENOSYS);
    }

    /**
     * Callback invoked when getting the file system statistics,
     * for instance when using `df` from command line.
     * 
     * @param {Object} context Context info of the calling process.
     * @param {Number} inode Inode number.
     * @param {Object} reply Reply instance.
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

    /**************************************************************
     * Database and conflict resolution functions
     **************************************************************/

    /**
     * Generate a random inode number.
     * NB: in production, use UUID or include unique per-client or per-site prefix.
     */
    getRandomIno() {
        return Math.floor(Math.random() * (INODE_HIGH - INODE_LOW) + INODE_LOW);
    }

    /**
     * Returns the Antidote update operations required to write
     * file or directory attributes.
     * 
     * @param {Object} attr File or directory metadata (attribute) object.
     */
    mdUpdate(attr) {
        let updates = [];
        const map = antidote.map(`inode_${attr.inode}`);
        updates.push(map.register('inode').set(attr.inode));
        updates.push(map.register('mode').set(attr.mode));
        updates.push(map.register('ctime').set(attr.ctime));
        updates.push(map.register('mtime').set(attr.mtime));
        updates.push(map.register('atime').set(attr.atime));
        updates.push(map.register('rdev').set(attr.rdev));
        updates.push(map.register('size').set(attr.size));
        updates.push(map.integer('nlink').set(attr.nlink));
        updates.push(map.register('uid').set(attr.uid));
        updates.push(map.register('gid').set(attr.gid));
        updates.push(map.register('isFile').set(attr.isFile));
        for (let name in attr.children) {
            if (attr.children.hasOwnProperty(name)) {
                // Always write a single inode per child name.
                assert(!Array.isArray(attr.children[name]));
                updates.push(map.map('children').set(name).add(attr.children[name]));
            }
        }
        for (let name in attr.hlinks) {
            if (attr.hlinks.hasOwnProperty(name)) {
                updates.push(map.map('hlinks').register(name).set(attr.hlinks[name]));
            }
        }
        return updates;
    }

    /**
     * Returns the Antidote update operations required to remove 
     * an inode metadata object.
     * 
     * @param {Object} attr File or directory metadata (attribute) object.
     */
    mdDelete(attr) {
        const map = antidote.map(`inode_${attr.inode}`);
        return map.removeAll([
            map.register('inode'),
            map.register('mode'),
            map.register('ctime'),
            map.register('mtime'),
            map.register('atime'),
            map.register('rdev'),
            map.register('size'),
            map.integer('nlink'),
            map.register('uid'),
            map.register('gid'),
            map.register('isFile'),
            map.map('children'),
            map.map('hlinks'),
        ]);
    }

    /**
     * Returns the Antidote update operation to remove a child from a
     * directory metadata object.
     * 
     * @param {Object} attr File or directory metadata (attribute) object.
     * @param {String} name Name of the child to delete.
     */
    mdDeleteChild(attr, name) {
        return antidote.map(`inode_${attr.inode}`).map('children').remove(
            antidote.set(name));
    }

    /**
     * Returns the Antidote update operations to remove a hlink reference
     * and decrement its count.
     * 
     * @param {Object} attr File or directory metadata (attribute) object.
     * @param {Number} ino Inode number of the parent.
     */
    mdDeleteHlink(attr, ino) {
        return Array.prototype.concat(
            antidote.map(`inode_${attr.inode}`).map('hlinks').remove(
                antidote.register(ino.toString())),
            antidote.map(`inode_${attr.inode}`).integer('nlink').increment(-1)
        );
    }

    /**
     * Returns the Antidote update operation to write a data object.
     * 
     * @param {Object} attr File or directory metadata (attribute) object.
     * @param {Object} data Data to be written.
     */
    dataUpdate(attr, data) {
        return antidote.register(`data_${attr.inode}`).set(new Buffer(data));
    }

    /**
     * Read, resolve conflicts and returns the metadata associated
     * to a certain inode number.
     * 
     * @param {Number} inode Inode number.
     */
    async readMd(inode) {
        let md = (await antidote.map(`inode_${inode}`).read()).toJsObject();
        if (!isEmptyObject(md)) {
            if (!md.children) {
                // Because empty maps are not returned by Antidote
                md.children = {};
            } else {
                for (let name in md.children) {
                    if (md.children.hasOwnProperty(name)) {
                        //log('reading children: ', name, ': ', md.children[name]);
                        assert(md.children[name].length != 0);
                        if (md.children[name].length == 1) {
                            md.children[name] = md.children[name][0];
                        } else {
                            // Merge naming conflicts
                            log('Merging naming conflicts: ', md);
                            await this.mergeNamingConflicts(md, name);
                        }
                    }
                }
            }

            if (!md.hlinks) md.hlinks = {};
        }
        return md;
    }

    /**
     * Read and returns the data associated to a certain inode number.
     * 
     * @param {Number} inode Inode number.
     */
    async readData(inode) {
        return await antidote.register(`data_${inode}`).read();
    }

    /**
     * Merge naming conflicts in a same directory:
     *  - multiple files with the same name: rename files $file-CONFLICT_$n
     *  - multiple files and one folder with the same name: rename files as above
     *  - multiple directories with the same name: merge directories
     * 
     * @param {Object} pmd Parent directory attributes object containing conflicts.
     * @param {String} name The name in parent directory's children having conflicts.
     */
    async mergeNamingConflicts(pmd, name) {

        // Get metadata of conflicting inodes
        let dirs = [], files = [];
        for (let i = 0; i < pmd.children[name].length; i++) {
            let childMd = await this.readMd(pmd.children[name][i]);
            if (childMd.isFile) files.push(childMd);
            else dirs.push(childMd);
        }
        log('Conflicts - dirs: ', dirs, '- files:', files);

        let updates = [];
        // If there are several conflicting directories
        if (dirs.length > 1) {
            // Merge conflicting directories
            log('Merging conflicting directories')
            let mergedDirMd = this.mergeDirs(dirs);
            dirs.forEach(function (dir, index, array) {
                // Remove conflicting inode references
                let indexChild = pmd.children[name].indexOf(dir.inode);
                pmd.children[name].splice(indexChild, 1);
                updates.push(
                    antidote.map(`inode_${pmd.inode}`).map('children').set(name).remove(dir.inode)
                );
                updates = updates.concat(this.mdDelete(dir));
            }, this);
            // Add reference to merged dir
            pmd.children[name] = mergedDirMd.inode;
            log('merged dir md:', mergedDirMd);
            updates.push(
                antidote.map(`inode_${pmd.inode}`).map('children').set(name).add(mergedDirMd.inode)
            );
            updates = updates.concat(this.mdUpdate(mergedDirMd));
        }

        // If there are several conflicting files, or just 1 and some directories
        if (files.length > 1 || (dirs.length + files.length > 1)) {
            // Rename files in parent's children list
            log('Renaming conflicting files')
            files.forEach(function (file, index, array) {
                const newname = name + '-CONFLICT_' + index;
                updates.push(
                    antidote.map(`inode_${pmd.inode}`).map('children').set(name).remove(file.inode),
                    antidote.map(`inode_${pmd.inode}`).map('children').set(newname).add(file.inode)
                );

                pmd.children[newname] = file.inode;
                const indexChild = pmd.children[name].indexOf(file.inode);
                pmd.children[name].splice(indexChild, 1);
            });

            if (dirs.length == 0) {
                // If the conflicting inodes are only files, 
                // we remove the original child name 
                // since conflicting files have been renamed
                updates.push(this.mdDeleteChild(pmd, name));
            }
        }

        // Write merged metadata
        await antidote.update(updates);
    }

    /**
     * Returns a new directory resulting from merging 
     * the directories given as input.
     * 
     * @param {Array} dirs Array of directories attributes to merge.
     */
    mergeDirs(dirs) {
        let mergedDirMd = new AttrDir(this.getRandomIno(), 2, null);

        let minMode = Number.MAX_SAFE_INTEGER;
        dirs.forEach(function (dir) {
            for (let name in dir.children) {
                if (dir.children.hasOwnProperty(name)) {
                    // TODO: overwrites conflicting children: recursive merge?
                    mergedDirMd.addChild(name, dir.children[name]);
                }
            }
            if (dir.mode < minMode) minMode = dir.mode;
        });
        mergedDirMd.mode = minMode;
        // XXX uid, gid?
        return mergedDirMd;
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

var argv = require('minimist')(process.argv.slice(2));

const mountPoint = argv.m ?
    argv.m : mkTmpDir();
const antidoteAddress = argv.a ?
    argv.a.split(':') : 'localhost:8087'.split(':');

let antidote = antidoteClient.connect(antidoteAddress[1], antidoteAddress[0]);
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
process.on('SIGTERM', unmount);
