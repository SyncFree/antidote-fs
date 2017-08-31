package eu.antidotedb.fs;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;
import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.Key;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.MapKey.MapReadResult;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

import static eu.antidotedb.client.Key.*;

// XXX Quirky behaviours in current Antidote implementation:
// a. can't create an empty register (it's not distinguishable from not existing one when reading)
// b. can't create an empty map: must create a dummy register in it

public class FsTree {

    static private AntidoteClient antidote;
    static private Bucket bucket;

    static private String BUCKET_LABEL = "antidote-fs";

    // name of register used as marker to differentiate empty maps (folders) from
    // not existing ones
    static private String DIRECTORY_MARKER = "DM";

    public static void initFsTree(String antidoteAdd) {
        String[] addrParts = antidoteAdd.split(":");
        antidote = new AntidoteClient(new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1])));
        bucket = Bucket.bucket(BUCKET_LABEL);
    }

    public static class Directory extends FsElement {
        public MapKey dirMap;

        public Directory(String name) {
            super(name);
            dirMap = map_aw(name);
            bucket.update(antidote.noTransaction(), dirMap.update(register(DIRECTORY_MARKER).assign("")));
        }

        public Directory(String name, Directory parent) {
            super(name, parent);
            dirMap = map_aw(name);
            bucket.update(antidote.noTransaction(), dirMap.update(register(DIRECTORY_MARKER).assign("")));
        }

        public synchronized void add(FsElement p) {
            if (p instanceof File)
                bucket.update(antidote.noTransaction(),
                        dirMap.update(register(p.name).assign(new String(((File) p).contents.array()))));
            else if (p instanceof Directory)
                bucket.update(antidote.noTransaction(),
                        dirMap.update(map_aw(p.name).update(register(DIRECTORY_MARKER).assign(""))));
        }

        public synchronized void deleteChild(FsElement child) {
            if (child instanceof File)
                bucket.update(antidote.noTransaction(), dirMap.removeKey(register(child.name)));
            else if (child instanceof Directory)
                bucket.update(antidote.noTransaction(), dirMap.removeKey(map_aw(child.name)));
        }

        public FsElement find(String path) {
            while (path.startsWith("/"))
                path = path.substring(1);

            if (path.equals(this.name) || path.isEmpty()) {
                // it's this element
                MapReadResult res = bucket.read(antidote.noTransaction(), dirMap);
                if (res.keySet().contains(register(DIRECTORY_MARKER)))
                    return this;
                else {
                    parent.deleteChild(this);
                    return null;
                }
            }

            synchronized (this) {
                if (!path.contains("/")) {
                    // it's in this folder
                    MapReadResult res = bucket.read(antidote.noTransaction(), dirMap);
                    for (Key<?> key : res.keySet())
                        if (key.getKey().toStringUtf8().equals(path))
                            if (key.getType().equals(CRDT_type.AWMAP))
                                return new Directory(path, this);
                            else if (key.getType().equals(CRDT_type.LWWREG))
                                return new File(path, this);
                    return null;
                } else {
                    // it's in a subfolder
                    String nextName = path.substring(0, path.indexOf("/"));
                    String rest = path.substring(path.indexOf("/"));
                    MapReadResult res = bucket.read(antidote.noTransaction(), dirMap);
                    for (Key<?> key : res.keySet())
                        if (key.getType().equals(CRDT_type.AWMAP) && nextName.equals(key.getKey().toStringUtf8()))
                            return new Directory(nextName, this).find(rest);
                }
            }
            return null;
        }

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFDIR | 0740);
        }

        public synchronized void mkdir(String newDirName) {
            bucket.update(antidote.noTransaction(),
                    dirMap.update(map_aw(newDirName).update(register(DIRECTORY_MARKER).assign(""))));
        }

        public synchronized void mkfile(String lastComponent) {
            bucket.update(antidote.noTransaction(), dirMap.update(register(lastComponent).assign("")));
        }

        public synchronized void read(Pointer buf, FuseFillDir filler) {
            MapReadResult res = bucket.read(antidote.noTransaction(), dirMap);
            for (Key<?> key : res.keySet())
                if (!key.getKey().toStringUtf8().equals(DIRECTORY_MARKER))
                    filler.apply(buf, key.getKey().toStringUtf8(), null, 0);
        }
    }

    public static class File extends FsElement {
        public ByteBuffer contents = ByteBuffer.allocate(0);

        public File(String name) {
            super(name);
        }

        public File(String name, Directory parent) {
            super(name, parent);
        }

        public File(String name, String text) {
            super(name);
            try {
                byte[] contentBytes = text.getBytes("UTF-8");
                contents = ByteBuffer.wrap(contentBytes);
            } catch (UnsupportedEncodingException e) {
                // Not going to happen
            }
        }

        public File find(String path) {
            while (path.startsWith("/"))
                path = path.substring(1);
            // it's this element
            if (path.equals(this.name)) {
                return this;
            } else
                return null;
        }

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFREG | 0740);
            String res = bucket.read(antidote.noTransaction(), parent.dirMap).get(register(name));
            stat.st_size.set(res.getBytes().length);
        }

        public int read(Pointer buffer, long size, long offset) {
            String res = bucket.read(antidote.noTransaction(), parent.dirMap).get(register(name));
            byte[] contentBytes = new byte[1];
            try {
                contentBytes = res.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                // Not going to happen
            }
            ByteBuffer contents = ByteBuffer.wrap(contentBytes);
            int bytesToRead = (int) Math.min(res.getBytes().length - offset, size);
            byte[] bytesRead = new byte[bytesToRead];
            synchronized (this) {
                contents.position((int) offset);
                contents.get(bytesRead, 0, bytesToRead);
                buffer.put(0, bytesRead, 0, bytesToRead);
                contents.position(0); // Rewind
            }
            return bytesToRead;
        }

        public synchronized void truncate(long size) {
            System.out.println("TRUNCATE NOT IMPLEMENTED");
            // XXX cut file to size (?)
        }

        public synchronized int write(Pointer buffer, long bufSize, long writeOffset) {
            String res = bucket.read(antidote.noTransaction(), parent.dirMap).get(register(name));
            byte[] contentBytes = new byte[1];
            try {
                contentBytes = res.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                // Not going to happen
            }
            ByteBuffer contents = ByteBuffer.wrap(contentBytes);
            int maxWriteIndex = (int) (writeOffset + bufSize);
            byte[] bytesToWrite = new byte[(int) bufSize];
            synchronized (this) {
                if (maxWriteIndex > contents.capacity()) {
                    // Need to create a new, larger buffer
                    ByteBuffer newContents = ByteBuffer.allocate(maxWriteIndex);
                    newContents.put(contents);
                    contents = newContents;
                }
                buffer.get(0, bytesToWrite, 0, (int) bufSize);
                contents.position((int) writeOffset);
                contents.put(bytesToWrite);
                contents.position(0); // Rewind
            }
            bucket.update(antidote.noTransaction(),
                    parent.dirMap.update(register(name).assign(new String(contents.array()))));
            return (int) bufSize;
        }
    }

    public static abstract class FsElement {
        protected String name;
        protected Directory parent;

        public FsElement(String name) {
            this(name, null);
        }

        public FsElement(String name, Directory parent) {
            this.name = name;
            this.parent = parent;
        }

        public synchronized void delete() {
            if (parent != null) {
                parent.deleteChild(this);
                parent = null;
            }
        }

        public abstract FsElement find(String path);

        public abstract void getattr(FileStat stat);

        public void rename(String newName) {
            while (newName.startsWith("/"))
                newName = newName.substring(1);
            name = newName;
        }
    }

}
