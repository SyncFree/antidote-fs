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

public class FsModel implements Runnable {

    private final AntidoteClient           antidote;
    private final Bucket                   bucket;

    private final MapKey                   pathsKey;
    private MapReadResult                  pathsMap;
    private final ScheduledExecutorService scheduler;

    static final private String            BUCKET_LABEL   = "antidote-fs";
    static final private String            PATHS_MAP      = "PATHS";

    // period for refreshing the path map
    static final public int                REFRESH_PERIOD = 5;

    static final private String            DIR_PREFIX     = "DIR_";
    static final private String            FILE_PREFIX    = "FILE_";

    // keys in each inode map
    static final private String            CONTENT        = "CONTENT";
    static final private String            SIZE           = "SIZE";
    static final private String            MODE           = "MODE";

    static final private String            CHILD_REGEXP   = "[" + separator + "]*";

    public FsModel(String antidoteAddr) {
        String[] addrParts = antidoteAddr.split(":");
        antidote = new AntidoteClient(
                new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1])));
        bucket = Bucket.bucket(BUCKET_LABEL);

        pathsKey = map_aw(PATHS_MAP);
        refreshPathMap();
        if (getPath(separator) == null) // create the root dir if not existing
            makeDir(separator);
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this, REFRESH_PERIOD, REFRESH_PERIOD, TimeUnit.SECONDS);
    }

    public void listDir(String path, Pointer buf, FuseFillDir filter) {
        for (Key<?> key : pathsMap.keySet()) {
            String keyStr = key.getKey().toStringUtf8();
            if (keyStr.startsWith(path) && keyStr.length() > path.length()
                    && !keyStr.replaceFirst(path + CHILD_REGEXP, "").contains(separator))
                filter.apply(buf, keyStr.substring(keyStr.lastIndexOf(separator) + 1), null, 0);
        }
    }

    public int writeFile(String path, Pointer buffer, long bufSize, long writeOffset) {
        String inodeKey = getPath(path);
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
        // contents.position(0); // Rewind

        bucket.update(antidote.noTransaction(),
                map_aw(inodeKey).update(register(CONTENT).assign(new String(contents.array())),
                        integer(SIZE).assign(contents.array().length)));

        return (int) bufSize;
    }

    public int readFile(String path, Pointer buffer, long size, long offset) {
        String inodeKey = getPath(path);
        String res = bucket.read(antidote.noTransaction(), map_aw(inodeKey)).get(register(CONTENT));

        byte[] contentBytes = res == null ? new byte[0] : res.getBytes();
        ByteBuffer contents = ByteBuffer.wrap(contentBytes);
        int bytesToRead = (int) Math.min(res.getBytes().length - offset, size);
        byte[] bytesRead = new byte[bytesToRead];
        contents.position((int) offset);
        contents.get(bytesRead, 0, bytesToRead);
        buffer.put(0, bytesRead, 0, bytesToRead);
        // contents.position(0); // Rewind
        return bytesToRead;
    }

    public boolean isDirectory(String path) {
        String inodeKey = getPath(path);
        if (inodeKey.startsWith(DIR_PREFIX))
            return true;
        else
            return false;
    }

    public void makeFile(String path) {
        // TODO bind all this into a tx
        String inodeKey = createFilePath(path);
        bucket.update(antidote.noTransaction(), map_aw(inodeKey)
                .update(integer(MODE).assign(FileStat.S_IFREG | 0740), 
                        integer(SIZE).assign(0L)));
    }

    public void makeDir(String path) {
        // TODO bind all this into a tx
        // XXX size of a dir?
        String inodeKey = createDirPath(path);
        bucket.update(antidote.noTransaction(), map_aw(inodeKey)
                .update(integer(MODE).assign(FileStat.S_IFDIR | 0740), 
                        integer(SIZE).assign(0L)));
    }

    public void rename(String oldPath, String newPath) {
        String inodeKey = getPath(oldPath);
        if (inodeKey.startsWith(DIR_PREFIX)) {

            // get all dir descendants
            HashMap<String, String> keysToCopy = new HashMap<>();
            for (Key<?> key : pathsMap.keySet()) {
                String keyStr = key.getKey().toStringUtf8();
                if (keyStr.startsWith(oldPath) && keyStr.length() > oldPath.length())
                    keysToCopy.put(keyStr.replaceFirst(oldPath + separator, ""),
                            (String) pathsMap.get(key));
            }

            try (InteractiveTransaction tx = antidote.startTransaction()) {
                // create new path and copy descendants
                bucket.update(tx, pathsKey.update(register(newPath).assign(inodeKey)));
                for (String k : keysToCopy.keySet())
                    bucket.update(tx, pathsKey
                            .update(register(newPath + separator + k).assign(keysToCopy.get(k))));

                // delete old key and old descendants
                bucket.update(tx, pathsKey.removeKey(register(oldPath)));
                for (String k : keysToCopy.keySet())
                    bucket.update(tx, pathsKey.removeKey(register(oldPath + separator + k)));

                tx.commitTransaction();
            }

        } else {
            try (InteractiveTransaction tx = antidote.startTransaction()) {
                bucket.update(tx, pathsKey.update(register(newPath).assign(inodeKey)));
                bucket.update(tx, pathsKey.removeKey(register(oldPath)));
                tx.commitTransaction();
            }
        }
        refreshPathMap();
    }

    @SuppressWarnings("unchecked") // XXX
    public void getAttr(String path, FileStat stat) {
        // TODO handle other attributes
        String inodeKey = getPath(path);
        MapReadResult res = bucket.read(antidote.noTransaction(), map_aw(inodeKey));
        long attr = (long) res.get(integer(MODE));
        long size = (long) res.get(integer(SIZE));
        stat.st_size.set(size);
        if (inodeKey.startsWith(DIR_PREFIX))
            stat.st_mode.set(FileStat.S_IFDIR | attr);
        else if (inodeKey.startsWith(FILE_PREFIX))
            stat.st_mode.set(FileStat.S_IFREG | attr);
    }

    public void truncate(String path, long offset) {
        // TODO
    }

    // --------------- Methods to manage the map of paths

    public String getPath(String path) {
        return pathsMap.get(register(path));
    }

    private String createFilePath(String path) {
        UUID fileId = UUID.randomUUID();
        String fileKey = FILE_PREFIX + fileId.toString();
        bucket.update(antidote.noTransaction(), pathsKey.update(register(path).assign(fileKey)));
        refreshPathMap();
        return fileKey;
    }

    private String createDirPath(String path) {
        UUID dirId = UUID.randomUUID();
        String dirKey = DIR_PREFIX + dirId.toString();
        bucket.update(antidote.noTransaction(), pathsKey.update(register(path).assign(dirKey)));
        refreshPathMap();
        return dirKey;
    }

    public void removePath(String path) {
        // TODO gc inode key
        bucket.update(antidote.noTransaction(), pathsKey.removeKey(register(path)));
        refreshPathMap();
    }

    @Override
    public void run() {
        refreshPathMap();
    }

    synchronized private void refreshPathMap() {
        pathsMap = bucket.read(antidote.noTransaction(), pathsKey);
    }
}
