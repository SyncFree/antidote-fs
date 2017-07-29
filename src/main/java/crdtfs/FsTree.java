package crdtfs;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

public class FsTree {

    public class Directory extends FsElement {
        private List<FsElement> contents = new ArrayList<>();

        public Directory(String name) {
            super(name);
        }

        public Directory(String name, Directory parent) {
            super(name, parent);
        }

        public synchronized void add(FsElement p) {
            contents.add(p);
            p.parent = this;
        }

        public synchronized void deleteChild(FsElement child) {
            contents.remove(child);
        }

        @Override
        public FsElement find(String path) {
            // it's this folder
            FsElement res = super.find(path);
            if (res != null)
                return res;

            while (path.startsWith("/"))
                path = path.substring(1);

            synchronized (this) {
                if (!path.contains("/")) {
                    // it's in this folder
                    for (FsElement p : contents)
                        if (p.name.equals(path))
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
        }

        public synchronized void mkfile(String lastComponent) {
            contents.add(new File(lastComponent, this));
        }

        public synchronized void read(Pointer buf, FuseFillDir filler) {
            for (FsElement p : contents) {
                filler.apply(buf, p.name, null, 0);
            }
        }
    }

    public class File extends FsElement {
        private ByteBuffer contents = ByteBuffer.allocate(0);

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

        @Override
        public void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFREG | 0777);
            stat.st_size.set(contents.capacity());
        }

        public int read(Pointer buffer, long size, long offset) {
            int bytesToRead = (int) Math.min(contents.capacity() - offset, size);
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
            if (size < contents.capacity()) {
                // Need to create a new, smaller buffer
                ByteBuffer newContents = ByteBuffer.allocate((int) size);
                byte[] bytesRead = new byte[(int) size];
                contents.get(bytesRead);
                newContents.put(bytesRead);
                contents = newContents;
            }
        }

        public int write(Pointer buffer, long bufSize, long writeOffset) {
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
            return (int) bufSize;
        }
    }

    public abstract class FsElement {
        private String name;
        private Directory parent;

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

        public FsElement find(String path) {
            while (path.startsWith("/"))
                path = path.substring(1);
            if (path.equals(name) || path.isEmpty())
                return this;
            return null;
        }

        public abstract void getattr(FileStat stat);

        public void rename(String newName) {
            while (newName.startsWith("/")) {
                newName = newName.substring(1);
            }
            name = newName;
        }
    }

}
