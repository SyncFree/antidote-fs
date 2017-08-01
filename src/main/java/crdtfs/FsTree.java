package crdtfs;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;
import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.Host;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.MapRef;
import eu.antidotedb.client.MapRef.MapReadResult;
import eu.antidotedb.client.ValueCoder;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

//XXX can't create empty file (not distinguishable from not present file when reading)
//XXX can't create an empty map: must create a dummy register in it

public class FsTree {

    static private AntidoteClient antidote = new AntidoteClient(new Host("127.0.0.1", 8087));
    static protected Bucket<String> bucket = Bucket.create("fsBucket");

    static private String DIRECTORY_MARKER = "DM";
    static private String NO_FILE_MARKER = "";

    public static class Directory extends FsElement {
        public MapRef<String> dirMapRef;

        public Directory(String name) {
            super(name);
            dirMapRef = bucket.map_aw(name);
            dirMapRef.register(DIRECTORY_MARKER).set(antidote.noTransaction(), "");
        }

        public Directory(String name, Directory parent) {
            super(name, parent);
            dirMapRef = bucket.map_aw(name);
            dirMapRef.register(DIRECTORY_MARKER).set(antidote.noTransaction(), "");
        }

        public synchronized void add(FsElement p) {
            if (p instanceof File)
                dirMapRef.register(p.name, ValueCoder.utf8String).set(antidote.noTransaction(),
                        new String(((File) p).contents.array()));
            else if (p instanceof Directory)
                dirMapRef.map_aw(p.name, ValueCoder.utf8String).register(DIRECTORY_MARKER).set(antidote.noTransaction(),
                        "");
        }

        public synchronized void deleteChild(FsElement child) {
            dirMapRef.removeKey(antidote.noTransaction(), MapKey.register(child.name));
        }

        public FsElement find(String path) {
            while (path.startsWith("/"))
                path = path.substring(1);
            // it's this element
            if (path.equals(this.name) || path.isEmpty()) {
                MapReadResult<String> res = dirMapRef.read(antidote.noTransaction());
                if (res.keySet().contains(DIRECTORY_MARKER))
                    return this;
                else {
                    parent.deleteChild(this);
                    return null;
                }                    
            }

            synchronized (this) {
                if (!path.contains("/")) {
                    // it's in this folder
                    MapReadResult<String> res = dirMapRef.read(antidote.noTransaction());
                    for (MapKey<String> key : res.mapKeySet())
                        if (key.getKey().equals(path))
                            if (key.getType().equals(CRDT_type.AWMAP))
                                return new Directory(path, this);
                            else if (key.getType().equals(CRDT_type.LWWREG))
                                return new File(path, this);
                    return null;
                } else {
                    // it's in a subfolder
                    String nextName = path.substring(0, path.indexOf("/"));
                    String rest = path.substring(path.indexOf("/"));
                    MapReadResult<String> res = dirMapRef.read(antidote.noTransaction());
                    for (MapKey<String> key : res.mapKeySet())
                        if (key.getType().equals(CRDT_type.AWMAP) && nextName.equals(key.getKey()))
                            return new Directory(nextName, this).find(rest);
                }
            }
            return null;
        }

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFDIR | 0777);
        }

        public synchronized void mkdir(String lastComponent) {
            dirMapRef.map_aw(lastComponent, ValueCoder.utf8String).register(DIRECTORY_MARKER)
                    .set(antidote.noTransaction(), "");
        }

        public synchronized void mkfile(String lastComponent) {
            dirMapRef.register(lastComponent, ValueCoder.utf8String).set(antidote.noTransaction(), "");
        }

        public synchronized void read(Pointer buf, FuseFillDir filler) {
            MapReadResult<String> res = dirMapRef.read(antidote.noTransaction());
            for (String element : res.keySet()) 
                if (!element.equals(DIRECTORY_MARKER))
                    filler.apply(buf, element, null, 0);
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
                String res = parent.dirMapRef.register(name).read(antidote.noTransaction());
                if (!res.equals(NO_FILE_MARKER))
                    return this;
                else
                    return null;
            } else
                return null;
        }

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFREG | 0777);
            String res = parent.dirMapRef.register(name).read(antidote.noTransaction());
            stat.st_size.set(res.getBytes().length);
        }

        public int read(Pointer buffer, long size, long offset) {
            String res = parent.dirMapRef.register(name).read(antidote.noTransaction());
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
            String res = parent.dirMapRef.register(name).read(antidote.noTransaction());
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
            parent.dirMapRef.register(name).set(antidote.noTransaction(), new String(contents.array()));
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
