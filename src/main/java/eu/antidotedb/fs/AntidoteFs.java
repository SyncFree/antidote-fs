package eu.antidotedb.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

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
    
    // TODO setup logging

    private final FsModel fs;

    public AntidoteFs(String antidoteAddress) {
        fs = new FsModel(antidoteAddress);
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        // System.out.println("**** CREATE " + path);
        if (fs.getInodeKey(path) != null)
            return -ErrorCodes.EEXIST();

        if (fs.isDirectory(FsModel.getParentPath(path))) {
            fs.makeFile(path);
            return 0;
        } else
            return -ErrorCodes.ENOENT();
    }

    @Override
    public int getattr(String path, FileStat stat) {
        // System.out.println("**** GETATTR " + path);
        if (fs.getInodeKey(path) != null) {
            fs.getAttr(path, stat);
            return 0;
        } else
            return -ErrorCodes.ENOENT();
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        // System.out.println("**** MAKEDIR " + path);
        if (fs.getInodeKey(path) != null)
            return -ErrorCodes.EEXIST();

        if (fs.isDirectory(FsModel.getParentPath(path))) {
            fs.makeDir(path);
            return 0;
        } else
            return -ErrorCodes.ENOENT();
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
            FuseFileInfo fi) {
        // System.out.println("**** READ " + path);
        if (fs.getInodeKey(path) == null)
            return -ErrorCodes.ENOENT();
        if (fs.isDirectory(path))
            return -ErrorCodes.EISDIR();

        return fs.readFile(path, buf, size, offset);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset,
            FuseFileInfo fi) {
        // System.out.println("**** READDIR " + path);
        if (fs.getInodeKey(path) == null)
            return -ErrorCodes.ENOENT();
        if (!fs.isDirectory(path))
            return -ErrorCodes.ENOTDIR();

        filter.apply(buf, ".", null, 0);
        filter.apply(buf, "..", null, 0);
        fs.listDir(path, buf, filter);
        return 0;
    }

    @Override
    public int rename(String oldPath, String newPath) {
        // System.out.println("**** RENAME " + oldPath + " " + newPath);
        if (fs.getInodeKey(oldPath) == null)
            return -ErrorCodes.ENOENT();

        if (fs.getInodeKey(FsModel.getParentPath(newPath)) == null)
            return -ErrorCodes.ENOENT();
        if (!fs.isDirectory(FsModel.getParentPath(newPath)))
            return -ErrorCodes.ENOTDIR();

        fs.rename(oldPath, newPath);
        return 0;
    }

    @Override
    public int rmdir(String path) {
        // System.out.println("**** RMDIR " + path);
        if (fs.getInodeKey(path) == null)
            return -ErrorCodes.ENOENT();
        if (!fs.isDirectory(path))
            return -ErrorCodes.ENOTDIR();

        fs.removePath(path);
        return 0;
    }

    @Override
    public int truncate(String path, long offset) {
        // System.out.println("**** TRUNCATE " + path);
        if (fs.getInodeKey(path) == null)
            return -ErrorCodes.ENOENT();
        if (fs.isDirectory(path))
            return -ErrorCodes.EISDIR();

        fs.truncate(path, offset);
        return 0;
    }

    @Override
    public int unlink(String path) {
        // System.out.println("**** UNLINK " + path);
        if (fs.getInodeKey(path) == null)
            return -ErrorCodes.ENOENT();

        fs.removePath(path);
        return 0;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset,
            FuseFileInfo fi) {
        // System.out.println("**** WRITE " + path);
        if (fs.getInodeKey(path) == null)
            return -ErrorCodes.ENOENT();
        if (fs.isDirectory(path))
            return -ErrorCodes.EISDIR();

        return fs.writeFile(path, buf, size, offset);
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
