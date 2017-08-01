package eu.antidotedb.fs;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import eu.antidotedb.fs.FsTree.Directory;
import eu.antidotedb.fs.FsTree.File;
import eu.antidotedb.fs.FsTree.FsElement;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class AntidoteFs extends FuseStubFS {

    public Directory rootDirectory;

    public AntidoteFs() {
        rootDirectory = new Directory("");
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        //System.out.println("**** CREATE");
        if (getFsElement(path) != null)
            return -ErrorCodes.EEXIST();

        FsElement parent = getFsParentElement(path);
        if (parent instanceof Directory) {
            ((Directory) parent).mkfile(getPathLastComponent(path));
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    @Override
    public int getattr(String path, FileStat stat) {
        //System.out.println("**** GETATTR");
        FsElement p = getFsElement(path);
        if (p != null) {
            p.getattr(stat);
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    private String getPathLastComponent(String path) {
        // remove trailing '/'
        while (path.substring(path.length() - 1).equals("/"))
            path = path.substring(0, path.length() - 1);

        if (path.isEmpty())
            return "";
        return path.substring(path.lastIndexOf("/") + 1);
    }

    private FsElement getFsParentElement(String path) {
        return rootDirectory.find(path.substring(0, path.lastIndexOf("/")));
    }

    private FsElement getFsElement(String path) {
        return rootDirectory.find(path);
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        //System.out.println("**** MAKEDIR");
        if (getFsElement(path) != null)
            return -ErrorCodes.EEXIST();

        FsElement parent = getFsParentElement(path);
        if (parent instanceof Directory) {
            ((Directory) parent).mkdir(getPathLastComponent(path));
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        //System.out.println("**** READ");
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        return ((File) p).read(buf, size, offset);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        //System.out.println("**** READDIR");
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        if (!(p instanceof Directory))
            return -ErrorCodes.ENOTDIR();

        filter.apply(buf, ".", null, 0);
        filter.apply(buf, "..", null, 0);
        ((Directory) p).read(buf, filter);
        return 0;
    }

    @Override
    public int rename(String path, String newName) {
        //System.out.println("**** RENAME");
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        FsElement newParent = getFsParentElement(newName);
        if (newParent == null)
            return -ErrorCodes.ENOENT();

        if (!(newParent instanceof Directory))
            return -ErrorCodes.ENOTDIR();

        p.delete();
        p.rename(newName.substring(newName.lastIndexOf("/")));
        ((Directory) newParent).add(p);
        return 0;
    }

    @Override
    public int rmdir(String path) {
        //System.out.println("**** RMDIR");
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        if (!(p instanceof Directory))
            return -ErrorCodes.ENOTDIR();

        p.delete();
        return 0;
    }

    // XXX probably not needed
    @Override
    public int truncate(String path, long offset) {
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        ((File) p).truncate(offset);
        return 0;
    }

    // XXX probably not needed
    @Override
    public int unlink(String path) {
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        p.delete();
        return 0;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        //System.out.println("**** WRITE");
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        return ((File) p).write(buf, size, offset);
    }

    public static void main(String[] args) {
        Path dir = Paths.get("/tmp/mnt");
        try {
            Files.createDirectory(dir);
        } catch (IOException e) {
            if (!(e instanceof FileAlreadyExistsException)) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        AntidoteFs stub = new AntidoteFs();
        try {
            stub.mount(dir, true, true);
        } finally {
            stub.umount();
        }
    }
}
