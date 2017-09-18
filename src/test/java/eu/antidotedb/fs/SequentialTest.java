package eu.antidotedb.fs;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static java.io.File.separator;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

/**
 * Test suite on sequential file system behavior.
 */
public class SequentialTest extends AntidoteFsAbstractTest {

    private static String                 TEST_ROOT_DIR = "antidote-fs";

    private static AntidoteFs             afs;
    private static Path                   rootDir;

    @ClassRule
    public static final DockerComposeRule docker        = DockerComposeRule.builder()
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

        File file = new File(rootDir.toAbsolutePath() + separator + getRandomString());
        assertFalse("file mustn't exist", file.exists());
        assertTrue("file hasn't been created", file.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
            writer.print(content1);
            writer.print(content2);
        }

        String text = Files.lines(file.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 + content2, text);

        assertTrue("file can't be deleted", file.delete());
        assertFalse("file mustn't exist", file.exists());
    }

    @Test
    public void emptyFileTest() throws Exception {
        File file = new File(rootDir.toAbsolutePath() + separator + getRandomString());
        assertFalse("file mustn't exist", file.exists());
        assertTrue("file hasn't been created", file.createNewFile());
        assertTrue("file must exist", file.exists());
        assertTrue("file can't be deleted", file.delete());
        assertFalse("file mustn't exist", file.exists());
    }

    @Test
    public void emptyDirectoryTest() throws Exception {
        Path dirPath = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir = new File(dirPath.toString());
        assertTrue("directory hasn't been created", dir.isDirectory() && dir.exists());
        assertTrue("directory can't be deleted", dir.delete());
        assertFalse("directory mustn't exist", dir.exists());
    }

    @Test
    public void basicDirCrudTest() throws Exception {
        Path dirPath = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir = new File(dirPath.toString());
        assertTrue("directory hasn't been created", dir.isDirectory());

        HashSet<Path> children = new HashSet<Path>();
        children.add(Files.createFile(Paths.get(dir.getAbsolutePath(), getRandomString())));
        children.add(Files.createFile(Paths.get(dir.getAbsolutePath(), getRandomString())));
        children.add(Files.createFile(Paths.get(dir.getAbsolutePath(), getRandomString())));
        children.add(Files.createDirectory(Paths.get(dir.getAbsolutePath(), getRandomString())));

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath)) {
            int count = 0;
            for (Path file : stream) {
                assertTrue(file.toString() + " was never created", children.contains(file));
                count++;
            }
            assertTrue("count of created files does not match", children.size() == count);
        } catch (IOException | DirectoryIteratorException x) {
            x.printStackTrace();
            fail("exception while listing the subdir: " + x.getMessage());
        }

        FileUtils.deleteDirectory(dir);
        assertFalse("directory mustn't exist", dir.exists());
        for (Path path : children)
            assertFalse("file mustn't exist", path.toFile().exists());
    }

    @Test
    public void moveFileTest() throws Exception {
        String content1 = getRandomString();
        String content2 = getRandomString();

        Path dirPath = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir = new File(dirPath.toString());
        assertTrue("directory hasn't been created", dir.isDirectory());

        File file = new File(rootDir.toAbsolutePath() + separator + getRandomString());
        assertTrue("file hasn't been created", file.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
            writer.print(content1);
            writer.print(content2);
        }

        // rename file inside the same directory
        File newFile = new File(
                rootDir.toAbsolutePath().toString() + separator + getRandomString());
        Files.move(file.toPath(), file.toPath().resolveSibling(newFile.getName()));

        // the new file exists
        assertTrue("file was not created", newFile.exists());
        // its content is the same as the original
        String text = Files.lines(newFile.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 + content2, text);
        // the original file is not there anymore
        assertFalse("file mustn't exist", file.exists());

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
    public void moveEmptyDirTest() throws Exception {
        Path dir1Path = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir1 = new File(dir1Path.toString());
        assertTrue("directory hasn't been created", dir1.isDirectory());

        Path dir2Path = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir2 = new File(dir2Path.toString());
        assertTrue("directory hasn't been created", dir2.isDirectory());

        // rename empty directory
        File newDir = new File(
                rootDir.toAbsolutePath().toString() + separator + getRandomString());
        Path newDirPath = Files.move(dir1Path, dir1Path.resolveSibling(newDir.getName()));
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
    public void moveNonEmptyDir() throws Exception {
        String content1 = getRandomString();
        String content2 = getRandomString();

        // dir1
        Path dir1Path = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir1 = new File(dir1Path.toString());
        assertTrue("directory hasn't been created", dir1.isDirectory());
        // file1 in dir1
        File file1 = new File(dir1Path.toAbsolutePath() + separator + getRandomString());
        assertTrue("file hasn't been created", file1.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file1)))) {
            writer.print(content1);
            writer.print(content2);
        }

        // rename non-empty directory
        File newDir1 = new File(
                rootDir.toAbsolutePath().toString() + separator + getRandomString());
        Files.move(dir1Path, dir1Path.resolveSibling(newDir1.getName()));
        // the new dir exists
        assertTrue("directory was not created", newDir1.isDirectory());
        // the original directory is not there anymore
        assertFalse("directory mustn't exist", dir1.exists());
        // the content of the file inside the directory we moved is preserved
        String text = Files
                .lines(Paths.get(newDir1.getAbsolutePath(), file1.getName()))
                .collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 +
                content2, text);
        // the original file is not there anymore
        assertFalse("file mustn't exist", file1.exists());

        Path dir2Path = Files
                .createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()));
        File dir2 = new File(dir2Path.toString());
        assertTrue("directory hasn't been created", dir2.isDirectory());

        // move non-empty directory into another directory
        FileUtils.moveDirectoryToDirectory(newDir1, dir2, true);
        File newDir1moved = new File(
                dir2.getAbsolutePath() + separator + newDir1.getName());
        assertTrue("directory was not created", newDir1moved.isDirectory());
        assertFalse("directory mustn't exist", newDir1.exists());
        text = Files
                .lines(Paths.get(newDir1moved.getAbsolutePath(), file1.getName()))
                .collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 +
                content2, text);
    }
}
