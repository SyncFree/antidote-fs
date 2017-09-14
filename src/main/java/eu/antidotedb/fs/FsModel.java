package eu.antidotedb.fs;

import static eu.antidotedb.client.Key.*;
import static java.io.File.separator;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.InteractiveTransaction;
import eu.antidotedb.client.Key;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.MapKey.MapReadResult;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

// TODO use byte registers instead of string for content
public class FsModel implements Runnable {

    private final AntidoteClient           antidote;
    private final Bucket                   bucket;
    private final int                      refreshPeriod;

    private final MapKey                   pathsKey;
    private MapReadResult                  pathsMap;
    private final ScheduledExecutorService pathsRefreshScheduler;

    static final private String            BUCKET_LABEL           = "antidote-fs";
    static final private String            PATHS_MAP              = "PATHS";

    // default period for refreshing the path map
    static final private int               DEFAULT_REFRESH_PERIOD = 5000;

    // prefixes of inode maps' keys
    static final private String            DIR_PREFIX             = "D_";
    static final private String            FILE_PREFIX            = "F_";

    // keys in each inode map
    static final private String            CONTENT                = "CONT";
    static final private String            SIZE                   = "SIZE";
    static final private String            MODE                   = "MODE";

    static final private String            SEP_REGEXP             = "[" + separator + "]*";

    public FsModel(String antidoteAddr, int rfsPeriod) {
        String[] addrParts = antidoteAddr.split(":");
        antidote = new AntidoteClient(
                new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1])));
        bucket = Bucket.bucket(BUCKET_LABEL);

        pathsKey = map_aw(PATHS_MAP);
        refreshPathsMap();
        if (getInodeKey(separator) == null) // create the root dir if not existing
            makeDir(separator);

        refreshPeriod = rfsPeriod > 0 ? rfsPeriod : DEFAULT_REFRESH_PERIOD;
        pathsRefreshScheduler = Executors.newScheduledThreadPool(1);
        pathsRefreshScheduler.scheduleAtFixedRate(this,
                refreshPeriod, refreshPeriod, TimeUnit.MILLISECONDS);
    }

    public void listDir(String path, Pointer buf, FuseFillDir filter) {
        for (Key<?> key : pathsMap.keySet()) {
            String keyStr = key.getKey().toStringUtf8();
            if (isChildPath(path, keyStr))
                filter.apply(buf, getNameFromPath(keyStr), null, 0);
        }
    }

    public int writeFile(String path, Pointer buffer, long bufSize, long writeOffset) {
        String inodeKey = getInodeKey(path);
        String res = bucket.read(antidote.noTransaction(), map_aw(inodeKey)).get(register(CONTENT));

        byte[] contentBytes = res == null ? new byte[0] : res.getBytes();
        ByteBuffer contents = ByteBuffer.wrap(contentBytes);
        int maxWriteIndex = (int) (writeOffset + bufSize);
        byte[] bytesToWrite = new byte[(int) bufSize];
        if (maxWriteIndex > contents.capacity()) {
            // Need to create a new, larger buffer
            ByteBuffer newContents = ByteBuffer.allocate(maxWriteIndex);
            newContents.put(contents);
            contents = newContents;
        }
        buffer.get(0, bytesToWrite, 0, (int) bufSize);
        contents.position((int) writeOffset);
        contents.put(bytesToWrite);

        bucket.update(antidote.noTransaction(),
                map_aw(inodeKey).update(
                        register(CONTENT).assign(new String(contents.array())),
                        integer(SIZE).assign(contents.array().length)));
        return (int) bufSize;
    }

    public int readFile(String path, Pointer buffer, long size, long offset) {
        String inodeKey = getInodeKey(path);
        String res = bucket.read(antidote.noTransaction(), map_aw(inodeKey)).get(register(CONTENT));

        byte[] contentBytes = res == null ? new byte[0] : res.getBytes();
        ByteBuffer contents = ByteBuffer.wrap(contentBytes);
        int bytesToRead = (int) Math.min(res.getBytes().length - offset, size);
        byte[] bytesRead = new byte[bytesToRead];
        contents.position((int) offset);
        contents.get(bytesRead, 0, bytesToRead);
        buffer.put(0, bytesRead, 0, bytesToRead);
        return bytesToRead;
    }

    public boolean isDirectory(String path) {
        String inodeKey = getInodeKey(path);
        if (inodeKey.startsWith(DIR_PREFIX))
            return true;
        else
            return false;
    }

    public void makeFile(String path) {
        String fileKey = FILE_PREFIX + UUID.randomUUID().toString();
        try (InteractiveTransaction tx = antidote.startTransaction()) {
            bucket.update(tx, pathsKey.update(register(path).assign(fileKey)));
            bucket.update(tx, map_aw(fileKey)
                    .update(integer(MODE).assign(FileStat.S_IFREG | 0740),
                            integer(SIZE).assign(0L)));
            tx.commitTransaction();
        }
        refreshPathsMap();
    }

    public void makeDir(String path) {
        // XXX size of a dir?
        String dirKey = DIR_PREFIX + UUID.randomUUID().toString();
        try (InteractiveTransaction tx = antidote.startTransaction()) {
            bucket.update(tx, pathsKey.update(register(path).assign(dirKey)));
            bucket.update(tx, map_aw(dirKey)
                    .update(integer(MODE).assign(FileStat.S_IFDIR | 0740),
                            integer(SIZE).assign(0L)));
            tx.commitTransaction();
        }
        refreshPathsMap();
    }

    public void rename(String oldPath, String newPath) {
        String inodeKey = getInodeKey(oldPath);
        if (inodeKey.startsWith(DIR_PREFIX)) { // move a dir

            // get all dir descendants
            HashMap<String, String> descToCopy = new HashMap<>();
            for (Key<?> key : pathsMap.keySet()) {
                String keyStr = key.getKey().toStringUtf8();
                if (isDescendantPath(oldPath, keyStr))
                    descToCopy.put(trimParentFromPath(oldPath, keyStr),
                            pathsMap.get(register(keyStr)));
            }

            try (InteractiveTransaction tx = antidote.startTransaction()) {
                // create new path
                bucket.update(tx, pathsKey.update(register(newPath).assign(inodeKey)));
                // copy descendants to the new path
                for (String k : descToCopy.keySet())
                    bucket.update(tx,
                            pathsKey.update(register(newPath + separator + k)
                                    .assign(descToCopy.get(k))));

                // delete old key
                bucket.update(tx, pathsKey.removeKey(register(oldPath)));
                // delete old descendants
                for (String k : descToCopy.keySet())
                    bucket.update(tx, pathsKey.removeKey(register(oldPath + separator + k)));

                tx.commitTransaction();
            }
        } else { // move a file
            try (InteractiveTransaction tx = antidote.startTransaction()) {
                bucket.update(tx, pathsKey.update(register(newPath).assign(inodeKey)));
                bucket.update(tx, pathsKey.removeKey(register(oldPath)));
                tx.commitTransaction();
            }
        }
        refreshPathsMap();
    }

    public void getAttr(String path, FileStat stat) {
        // TODO handle other attributes
        String inodeKey = getInodeKey(path);
        MapReadResult res = bucket.read(antidote.noTransaction(), map_aw(inodeKey));
        // XXX remove casting once IntegerKey typing is published
        long mode = (long) res.get(integer(MODE));
        long size = (long) res.get(integer(SIZE));
        stat.st_size.set(size);
        if (inodeKey.startsWith(DIR_PREFIX))
            stat.st_mode.set(FileStat.S_IFDIR | mode);
        else if (inodeKey.startsWith(FILE_PREFIX))
            stat.st_mode.set(FileStat.S_IFREG | mode);
    }

    public void truncate(String path, long offset) {
        // TODO
    }

    public String getInodeKey(String path) {
        return pathsMap.get(register(path));
    }

    public void removePath(String path) {
        // TODO gc inode key
        bucket.update(antidote.noTransaction(), pathsKey.removeKey(register(path)));
        refreshPathsMap();
    }

    @Override
    public void run() {
        refreshPathsMap();
    }

    synchronized private void refreshPathsMap() {
        pathsMap = bucket.read(antidote.noTransaction(), pathsKey);
    }

    // --------------- Static methods to manage path strings

    private static boolean isChildPath(String parent, String child) {
        return isDescendantPath(parent, child)
                && !child.replaceFirst(parent + SEP_REGEXP, "").contains(separator);
    }

    private static boolean isDescendantPath(String ancestor, String descendant) {
        return descendant.startsWith(ancestor) && descendant.length() > ancestor.length();
    }

    public static String getParentPath(String path) {
        if (!path.substring(1).contains(separator)) // in the root folder
            return separator;
        else
            return path.substring(0, path.lastIndexOf(separator));
    }

    private static String getNameFromPath(String path) {
        return path.substring(path.lastIndexOf(separator) + 1);
    }

    private static String trimParentFromPath(String parent, String path) {
        return path.replaceFirst("^" + parent + SEP_REGEXP, "");
    }
}
