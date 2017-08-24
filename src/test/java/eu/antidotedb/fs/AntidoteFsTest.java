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
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

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
    public void basicDirCrudTest() throws Exception {
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions
                .asFileAttribute(PosixFilePermissions.fromString("rwxr-x---"));
        Path newdirPath = Files.createDirectory(Paths.get(rootDir.toAbsolutePath().toString(), getRandomString()), attr);
        File newdir = new File(newdirPath.toString());
        assertTrue("directory hasn't been created", newdir.isDirectory() && newdir.exists());

        HashSet<Path> children = new HashSet<Path>();
        children.add(Files.createFile(Paths.get(newdir.getAbsolutePath(), getRandomString()), attr));
        children.add(Files.createFile(Paths.get(newdir.getAbsolutePath(), getRandomString()), attr));
        children.add(Files.createFile(Paths.get(newdir.getAbsolutePath(), getRandomString()), attr));
        children.add(Files.createDirectory(Paths.get(newdir.getAbsolutePath(), getRandomString()), attr));

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

}
