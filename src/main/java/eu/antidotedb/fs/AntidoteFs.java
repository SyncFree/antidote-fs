package eu.antidotedb.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

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

import static java.io.File.separator;

/**
 * An AntidoteFs instance mounts and manages a single mount point of an
 * Antidote-based file system. Its command line parameters are:
 * <ul>
 * <li>-d / --dir: the path of the local mount point (if not existing, it will
 * be created)</li>
 * <li>-a / --antidote: the address of the Antidote database, formatted as
 * &lt;IPAddress:Port&gt;</li>
 * </ul>
 */
public class AntidoteFs extends FuseStubFS {

    private static class Args {
        @Parameter(names = { "--dir", "-d" })
        private String fsDir;
        @Parameter(names = { "--antidote", "-a" })
        private String antidoteAddress;
    }

    public Directory rootDirectory;

    public AntidoteFs(String antidoteAddress) {
        FsTree.initFsTree(antidoteAddress);
        rootDirectory = new Directory("");
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        // System.out.println("**** CREATE " + path);
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
        // System.out.println("**** GETATTR " + path);
        FsElement p = getFsElement(path);
        if (p != null) {
            p.getattr(stat);
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    private String getPathLastComponent(String path) {
        // remove trailing '/'
        while (path.substring(path.length() - 1).equals(separator))
            path = path.substring(0, path.length() - 1);

        if (path.isEmpty())
            return "";
        return path.substring(path.lastIndexOf(separator) + 1);
    }

    private FsElement getFsParentElement(String path) {
        return rootDirectory.find(path.substring(0, path.lastIndexOf(separator)));
    }

    private FsElement getFsElement(String path) {
        return rootDirectory.find(path);
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        // System.out.println("**** MAKEDIR " + path);
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
        // System.out.println("**** READ " + path);
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();
        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        return ((File) p).read(buf, size, offset);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        // System.out.println("**** READDIR " + path);
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();
        if (!(p instanceof Directory))
            return -ErrorCodes.ENOTDIR();

        // XXX is this portable?
        filter.apply(buf, ".", null, 0);
        filter.apply(buf, "..", null, 0);
        ((Directory) p).read(buf, filter);
        return 0;
    }

    @Override
    public int rename(String oldPath, String newPath) {
        // System.out.println("**** RENAME " + oldPath + " " + newPath);
        FsElement oldElement = getFsElement(oldPath);
        if (oldElement == null)
            return -ErrorCodes.ENOENT();

        FsElement newParent = getFsParentElement(newPath);
        if (newParent == null)
            return -ErrorCodes.ENOENT();
        if (!(newParent instanceof Directory))
            return -ErrorCodes.ENOTDIR();

        if (oldElement instanceof File) {
            File newFile = new File((File) oldElement);
            newFile.rename(getPathLastComponent(newPath));
            ((Directory) newParent).add(newFile);
        } else if (oldElement instanceof Directory) {
            Directory newDir = new Directory((Directory) oldElement);
            newDir.rename(getPathLastComponent(newPath));
            ((Directory) newParent).add(newDir);
        }
        
        oldElement.delete();
        return 0;
    }

    @Override
    public int rmdir(String path) {
        // System.out.println("**** RMDIR " + path);
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();
        if (!(p instanceof Directory))
            return -ErrorCodes.ENOTDIR();

        p.delete();
        return 0;
    }

    // XXX not commonly needed?
    @Override
    public int truncate(String path, long offset) {
        // System.out.println("**** TRUNCATE " + path);
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();
        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        ((File) p).truncate(offset);
        return 0;
    }

    @Override
    public int unlink(String path) {
        // System.out.println("**** UNLINK " + path);
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        p.delete();
        return 0;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        // System.out.println("**** WRITE " + path);
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();
        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        return ((File) p).write(buf, size, offset);
    }

    public static void main(String[] args) {
        Args ar = new Args();
        JCommander.newBuilder().addObject(ar).build().parse(args);
        Path rootPath = Paths.get(ar.fsDir);
        AntidoteFs stub = null;
        try {
            if (Files.notExists(rootPath))
                Files.createDirectory(rootPath);
            stub = new AntidoteFs(ar.antidoteAddress);
            stub.mount(rootPath, true, true);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            if (stub != null)
                stub.umount();
        }
    }
}
