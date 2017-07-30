package crdtfs;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.Host;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.MapRef;
import eu.antidotedb.client.MapRef.MapReadResult;
import eu.antidotedb.client.RegisterRef;
import eu.antidotedb.client.ValueCoder;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

public class FsTree {

    static private AntidoteClient antidote = new AntidoteClient(new Host("127.0.0.1", 8087));
    static protected Bucket<String> bucket = Bucket.create("fsBucket");

    public static class Directory extends FsElement {
        private List<FsElement> contents = new ArrayList<>();
        private MapRef<String> mapRef;

        public Directory(String name) {
            super(name);
            mapRef = bucket.map_aw(name);
        }

        public Directory(String name, Directory parent) {
            super(name, parent);
            mapRef = bucket.map_aw(name);
        }

        public synchronized void add(FsElement p) {
            // XXX need to set an initial value or reset or can create w/o it?
            if (p instanceof File) 
                mapRef.register(p.name, ValueCoder.utf8String).reset(antidote.noTransaction());
            else 
                mapRef.map_aw(p.name, ValueCoder.utf8String).reset(antidote.noTransaction());
            contents.add(p);
            p.parent = this;
        }

        public synchronized void deleteChild(FsElement child) {
            mapRef.removeKey(antidote.noTransaction(), MapKey.register(child.name));
            contents.remove(child);
        }

        public FsElement find(String path) {
            while (path.startsWith("/"))
                path = path.substring(1);
            // it's this element
            if (path.equals(this.name) || path.isEmpty()) {
                MapReadResult<String> res = mapRef.read(antidote.noTransaction());
                // XXX how to verify that it's still there on Antidote ?
                return this;
            }

            synchronized (this) {
                if (!path.contains("/")) {
                    // it's in this folder
                    for (FsElement p : contents)
                        if (p.name.equals(path))
                            // XXX how to verify that it's still there on Antidote ?
                            // can be file or folder
                            return p;
                    return null;
                } else {
                    // it's in a subfolder
                    String nextName = path.substring(0, path.indexOf("/"));
                    String rest = path.substring(path.indexOf("/"));
                    for (FsElement p : contents)
                        if (p.name.equals(nextName))
                            return p.find(rest);
                }
            }
            return null;
        }

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFDIR | 0777);
        }

        public synchronized void mkdir(String lastComponent) {
            contents.add(new Directory(lastComponent, this));
            mapRef.map_aw(lastComponent, ValueCoder.utf8String).reset(antidote.noTransaction());
        }

        public synchronized void mkfile(String lastComponent) {
            contents.add(new File(lastComponent, this));
            mapRef.register(lastComponent, ValueCoder.utf8String).reset(antidote.noTransaction());
        }

        public synchronized void read(Pointer buf, FuseFillDir filler) {
            MapReadResult<String> res = mapRef.read(antidote.noTransaction());
            for (String element : res.keySet())
                filler.apply(buf, element, null, 0);
            //for (FsElement p : contents)
            //    filler.apply(buf, p.name, null, 0);
        }
    }

    public static class File extends FsElement {
        //private ByteBuffer contents = ByteBuffer.allocate(0);
        private RegisterRef<String> regRef;

        public File(String name) {
            super(name);
            regRef = bucket.register(name);
        }

        public File(String name, Directory parent) {
            super(name, parent);
            regRef = bucket.register(name);
        }

        public File(String name, String text) {
            super(name);
            regRef = bucket.register(name);
            regRef.set(antidote.noTransaction(), text);
//            try {
//                byte[] contentBytes = text.getBytes("UTF-8");
//                contents = ByteBuffer.wrap(contentBytes);
//            } catch (UnsupportedEncodingException e) {
//                // Not going to happen
//            }
        }

        public File find(String path) {
            while (path.startsWith("/"))
                path = path.substring(1);
            // it's this element
            if (path.equals(this.name)) {
                regRef.read(antidote.noTransaction());
                // XXX how to verify that it's still there on Antidote ?
                return this;
            } else
                return null;
        }

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFREG | 0777);
            String res = regRef.read(antidote.noTransaction());
            stat.st_size.set(res.getBytes().length);
        }

        public int read(Pointer buffer, long size, long offset) {
            String res = regRef.read(antidote.noTransaction());
            byte[] contentBytes = new byte[1];
            try {
                contentBytes = res.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                // 
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
            /*if (size < contents.capacity()) {
                // Need to create a new, smaller buffer
                ByteBuffer newContents = ByteBuffer.allocate((int) size);
                byte[] bytesRead = new byte[(int) size];
                contents.get(bytesRead);
                newContents.put(bytesRead);
                contents = newContents;
            }*/
        }

        public int write(Pointer buffer, long bufSize, long writeOffset) {
            String res = regRef.read(antidote.noTransaction());
            byte[] contentBytes = new byte[1];
            try {
                contentBytes = res.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                // 
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
            regRef.set(antidote.noTransaction(), new String(contents.array()));
            return (int) bufSize;
        }
    }

    public static abstract class FsElement {
        protected String name;
        private Directory parent;

        public FsElement(String name) {
            this(name, null);
        }

        public FsElement(String name, Directory parent) {
            this.name = name;
            this.parent = parent;
        }

        public synchronized void delete() {
            // TODO rm element from parent directory
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
