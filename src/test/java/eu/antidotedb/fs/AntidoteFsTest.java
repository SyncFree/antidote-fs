package eu.antidotedb.fs;

import static eu.antidotedb.client.Key.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.Key;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.MapKey.MapReadResult;

public class AntidoteFsTest extends AntidoteFsAbstractTest {

    private static String TEST_ROOT_DIR = "antidote-fs";

    private static AntidoteFs afs;
    private static Path rootDir;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-antidote-single_host.yml")
            .waitingForService("antidote", HealthChecks.toHaveAllPortsOpen()).build();

    @BeforeClass
    public static void mountFs() throws IOException, InterruptedException {
        DockerPort antidoteContainer = docker.containers().container("antidote").port(8087);
        afs = new AntidoteFs(antidoteContainer.inFormat("$HOST:$EXTERNAL_PORT"));
        rootDir = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs, rootDir);
    }

    @AfterClass
    public static void unmountFs() {
        afs.umount();
    }

    @Test
    public void basicFileCrudTest() throws Exception {
        String content1 = getRandomString();
        String content2 = getRandomString();

        File fileOne = new File(rootDir.toAbsolutePath() + File.separator + getRandomString());
        assertFalse("file mustn't exist", fileOne.exists());
        assertTrue("file hasn't been created", fileOne.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileOne)))) {
            writer.print(content1);
            writer.print(content2);
        }

        String text = Files.lines(fileOne.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 + content2, text);

        assertTrue("file can't be deleted", fileOne.delete());
        assertFalse("file mustn't exist", fileOne.exists());
    }

    @Test
    public void emptyFileTest() throws Exception {
        File fileOne = new File(rootDir.toAbsolutePath() + File.separator + getRandomString());
        assertFalse("file mustn't exist", fileOne.exists());
        assertTrue("file hasn't been created", fileOne.createNewFile());
        assertTrue("file must exist", fileOne.exists());
        assertTrue("file can't be deleted", fileOne.delete());
        assertFalse("file mustn't exist", fileOne.exists());
    }

    @Test
    public void emptyDirectoryTest() throws Exception {
        Path newdirPath = Files.createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File newdir = new File(newdirPath.toString());
        assertTrue("directory hasn't been created", newdir.isDirectory() && newdir.exists());
        assertTrue("directory can't be deleted", newdir.delete());
        assertFalse("directory mustn't exist", newdir.exists());
    }

    @Test
    public void basicDirCrudTest() throws Exception {
        Path newdirPath = Files.createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File newdir = new File(newdirPath.toString());
        assertTrue("directory hasn't been created", newdir.isDirectory());

        HashSet<Path> children = new HashSet<Path>();
        children.add(Files.createFile(Paths.get(newdir.getAbsolutePath(), getRandomString())));
        children.add(Files.createFile(Paths.get(newdir.getAbsolutePath(), getRandomString())));
        children.add(Files.createFile(Paths.get(newdir.getAbsolutePath(), getRandomString())));
        children.add(Files.createDirectory(Paths.get(newdir.getAbsolutePath(), getRandomString())));

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(newdirPath)) {
            int count = 0;
            for (Path file : stream) {
                assertTrue(file.toString() + " was never created", children.contains(file));
                count++;
            }
            assertTrue("count of created files does not match", children.size() == count);
        } catch (IOException | DirectoryIteratorException x) {
            x.printStackTrace();
            fail("exception while listing the subdir");
        }

        for (Path path : children)
            path.toFile().delete();

        assertTrue("directory can't be deleted", newdir.delete());
        assertFalse("directory mustn't exist", newdir.exists());
    }

    @Test
    public void mvFileTest() throws Exception {
        String content1 = getRandomString();
        String content2 = getRandomString();

        Path dirPath = Files.createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir = new File(dirPath.toString());
        assertTrue("directory hasn't been created", dir.isDirectory());

        File fileOne = new File(rootDir.toAbsolutePath() + File.separator + getRandomString());
        assertTrue("file hasn't been created", fileOne.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileOne)))) {
            writer.print(content1);
            writer.print(content2);
        }

        // rename file inside the same directory
        File newFile = new File(rootDir.toAbsolutePath().toString() + File.separator + getRandomString());
        Files.move(fileOne.toPath(), fileOne.toPath().resolveSibling(newFile.getName()));

        // the new file exists
        assertTrue("file was not created", newFile.exists());
        // its content is the same as the original
        String text = Files.lines(newFile.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 + content2, text);
        // the original file is not there anymore
        assertFalse("file mustn't exist", fileOne.exists());

        // mv file into dir
        Files.move(newFile.toPath(), dirPath.resolve(newFile.getName()));

        // the new file exists
        assertTrue(dir.listFiles()[0].getName().equals(newFile.getName()));
        // its content is the same as the original
        text = Files.lines(dir.listFiles()[0].toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 + content2, text);
        // the original file is not there anymore
        assertFalse("file mustn't exist", newFile.exists());
        
        assertTrue("directory can't be deleted", dir.delete());
        assertFalse("directory mustn't exist", dir.exists());
        
        // XXX test ATOMIC_MOVE and REPLACE_EXISTING options of Files.move
    }
    
    @Test
    public void mvEmptyDirTest() throws Exception {
        Path dirPath1 = Files.createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir1 = new File(dirPath1.toString());
        assertTrue("directory hasn't been created", dir1.isDirectory());
        
        Path dir2Path = Files.createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir2 = new File(dir2Path.toString());
        assertTrue("directory hasn't been created", dir2.isDirectory());
        
        // rename empty directory 
        File newDir = new File(rootDir.toAbsolutePath().toString() + File.separator + getRandomString());
        Path newDirPath = Files.move(dirPath1, dirPath1.resolveSibling(newDir.getName()));
        // the new dir exists
        assertTrue("directory was not created", newDir.isDirectory());
        // the original directory is not there anymore
        assertFalse("directory mustn't exist", dir1.exists());
        
        // move empty directory in another directory
        Path newDir1Path = Files.move(newDirPath, dir2Path.resolve(newDir.getName()));
        File newDir1 = new File(newDir1Path.toString());
        // the new dir exists
        assertTrue("directory was not created", newDir1.isDirectory());
        // the original directory is not there anymore
        assertFalse("directory mustn't exist", newDir.exists());

        assertTrue("directory can't be deleted", dir2.delete());
        assertFalse("directory mustn't exist", dir2.exists());
        
        // TODO test ATOMIC_MOVE and REPLACE_EXISTING options of Files.move
    }
    
    @Test
    public void mvNonEmptyDir() throws Exception {
        
    }

    /*
     * Tests that objects embedded on maps in Antidote are actually deleted, and not
     * just unlinked.
     */
    @Ignore
    @Test
    public void antidoteDeletionTest() throws Exception {
        DockerPort antidoteContainer = docker.containers().container("antidote").port(8087);
        Bucket bucket = Bucket.bucket("test");
        String[] addrParts = antidoteContainer.inFormat("$HOST:$EXTERNAL_PORT").split(":");
        AntidoteClient antidote = new AntidoteClient(
                new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1])));

        // create /ROOT/A/file1 /ROOT/A/B
        // MapKey rootmap = bucket.update(antidote.noTransaction(), Key.map_aw("ROOT"));
        MapKey rootmap = Key.map_aw("ROOT");
        bucket.update(antidote.noTransaction(), rootmap.update(map_aw("A").update(register("file1").assign(""))));
        bucket.update(antidote.noTransaction(),
                rootmap.update(map_aw("A").update(map_aw("B").update(register("marker").assign("")))));

        MapReadResult res = bucket.read(antidote.noTransaction(), rootmap);
        assertTrue(res.keySet().contains(map_aw("A")));

        // remove /ROOT/A
        bucket.update(antidote.noTransaction(), rootmap.removeKey(map_aw("A")));
        res = bucket.read(antidote.noTransaction(), rootmap);
        assertFalse(res.keySet().contains(map_aw("A")));

        // re-create /ROOT/A
        bucket.update(antidote.noTransaction(), rootmap.update(map_aw("A").update(register("marker").assign(""))));

        // check that /ROOT/A/file1 /ROOT/A/B don't exist
        res = bucket.read(antidote.noTransaction(), rootmap).get(map_aw("A"));
        assertFalse(res.keySet().contains(map_aw("B")));
        assertFalse(res.keySet().contains(register("file1")));
    }
}
