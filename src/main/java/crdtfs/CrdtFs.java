package crdtfs;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.protobuf.ByteString;

import crdtfs.FsTree.Directory;
import crdtfs.FsTree.File;
import crdtfs.FsTree.FsElement;
import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.AntidoteStaticTransaction;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.Host;
import eu.antidotedb.client.MapRef;
import eu.antidotedb.client.ValueCoder;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class CrdtFs extends FuseStubFS {

    private AntidoteClient antidote;
    // private MapRef<String> rootDirectory;
    private Directory rootDirectory;

    public CrdtFs() {
        rootDirectory = new Directory("");
        rootDirectory.add(new File("Sample file.txt", "Hello there, feel free to look around.\n"));
        rootDirectory.add(new Directory("Sample directory"));
        Directory dirWithFiles = new Directory("Directory with files");
        rootDirectory.add(dirWithFiles);
        dirWithFiles.add(new File("hello.txt", "This is some sample text.\n"));
        dirWithFiles.add(new File("hello again.txt", "This another file with text in it! Oh my!\n"));
        Directory nestedDirectory = new Directory("Sample nested directory");
        dirWithFiles.add(nestedDirectory);
        nestedDirectory.add(new File("So deep.txt", "Man, I'm like, so deep in this here file structure.\n"));

        // antidote = new AntidoteClient(new Host("127.0.0.1", 8087));

        // create folders and files
        /*
         * Bucket<String> bucket = Bucket.create("fsBucket"); AntidoteStaticTransaction
         * tx = antidote.createStaticTransaction(); rootDirectory =
         * bucket.map_aw("root"); rootDirectory.map_aw("dirA").register("file1",
         * ValueCoder.bytestringEncoder).set(tx,
         * ByteString.copyFrom("Ciao, sono file1".getBytes()));
         * rootDirectory.register("hello.txt", ValueCoder.bytestringEncoder).set(tx,
         * ByteString.copyFrom("Ciao, sono hello.txt".getBytes()));
         * tx.commitTransaction();
         */
    }

    /*
     * // get file attributes
     * 
     * @Override public int getattr(String path, FileStat stat) {
     * System.out.println("getattr path: " + path);
     * 
     * int resp = 0; switch (StringUtils.countMatches(path, "/")) {
     * 
     * case 1: if (Objects.equals(path, "/")) { // root folder
     * stat.st_mode.set(FileStat.S_IFDIR | 0755); stat.st_nlink.set(2); } else { //
     * files or folder in root resp = -ErrorCodes.ENOENT();
     * MapRef.MapReadResult<String> res = rootFolder.read(antidote.noTransaction());
     * for (MapKey<String> key : res.mapKeySet()) if
     * (key.getKey().equals(path.substring(1))) { if
     * (key.getType().equals(CRDT_type.AWMAP)) { stat.st_mode.set(FileStat.S_IFDIR |
     * 0755); stat.st_nlink.set(2); } else { stat.st_mode.set(FileStat.S_IFREG |
     * 0444); stat.st_nlink.set(1); stat.st_size.set(res.get(key,
     * ResponseDecoder.register()).get().getBytes().length); } resp = 0; break; } }
     * break;
     * 
     * case 2: // dirA folder if
     * (path.substring(path.lastIndexOf("/")).equals("file1")) {
     * stat.st_mode.set(FileStat.S_IFREG | 0444); stat.st_nlink.set(1);
     * stat.st_size.set(100); // XXX // MapRef.MapReadResult<String> res =
     * rootFolder.read(antidote.noTransaction()); // res.map_aw("dirA",
     * ValueCoder.bytestringEncoder).register("file1",
     * ValueCoder.bytestringEncoder).getValue(); } else resp = -ErrorCodes.ENOENT();
     * break;
     * 
     * default: resp = -ErrorCodes.ENOENT(); break; } return resp; }
     * 
     * // ls dir
     * 
     * @Override public int readdir(String path, Pointer buf, FuseFillDir
     * filter, @off_t long offset, FuseFileInfo fi) {
     * 
     * System.out.println("readdir path: " + path); filter.apply(buf, ".", null, 0);
     * filter.apply(buf, "..", null, 0);
     * 
     * if ("/".equals(path)) { MapRef.MapReadResult<String> res =
     * rootFolder.read(antidote.noTransaction()); for (String key : res.keySet())
     * filter.apply(buf, key, null, 0); } else if ("/dirA".equals(path)){
     * filter.apply(buf, "file1", null, 0); } else return -ErrorCodes.ENOENT();
     * return 0; }
     * 
     * // open file
     * 
     * @Override public int open(String path, FuseFileInfo fi) { // if
     * (!HELLO_PATH.equals(path)) { // return -ErrorCodes.ENOENT(); // }
     * System.out.println("open path: " + path); return 0; }
     * 
     * // read file
     * 
     * @Override public int read(String path, Pointer buf, @size_t long size, @off_t
     * long offset, FuseFileInfo fi) { // if (!HELLO_PATH.equals(path)) { // return
     * -ErrorCodes.ENOENT(); // } System.out.println("reading path: " + path);
     * 
     * // byte[] bytes = HELLO_STR.getBytes(); // int length = bytes.length; // if
     * (offset < length) { // if (offset + size > length) { // size = length -
     * offset; // } // buf.put(0, bytes, 0, bytes.length); // } else { // size = 0;
     * // } return (int) size; }
     */

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
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
        FsElement p = getFsElement(path);
        if (p == null)
            return -ErrorCodes.ENOENT();

        if (!(p instanceof File))
            return -ErrorCodes.EISDIR();

        return ((File) p).read(buf, size, offset);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
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
        CrdtFs stub = new CrdtFs();
        try {
            stub.mount(dir, true, true);
        } finally {
            stub.umount();
        }
    }
}
