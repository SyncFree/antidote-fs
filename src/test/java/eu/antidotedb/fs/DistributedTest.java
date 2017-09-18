package eu.antidotedb.fs;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.State;

/**
 * Test suite with distributed file system clients.
 */
public class DistributedTest extends AntidoteFsAbstractTest {

    private static String                 TEST_ROOT_DIR    = "antidote-fs";

    private static AntidoteFs             afs1;
    private static AntidoteFs             afs2;
    private static AntidoteFs             afs3;
    private static Path                   rootDir1;
    private static Path                   rootDir2;
    private static Path                   rootDir3;

    private static int                    refreshPeriod    = 200;
    private static int                    propagationDelay = 500;

    @ClassRule
    public static final DockerComposeRule docker           = DockerComposeRule.builder()
            .file("src/test/resources/docker-antidote-3dcs.yml").build();

    @BeforeClass
    public static void mountFs() throws IOException, InterruptedException {
        // wait for cluster setup (about 1.5s from here)
        while (!docker.containers().container("link").state().equals(State.HEALTHY))
            Thread.sleep(100);

        /*
         * 3 different mount points, each attached to a different Antidote server of the
         * same cluster.
         */
        DockerPort antidoteContainer1 = docker.containers().container("antidote1").port(8087);
        afs1 = new AntidoteFs(antidoteContainer1.inFormat("$HOST:$EXTERNAL_PORT"), refreshPeriod);
        rootDir1 = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs1, rootDir1);

        DockerPort antidoteContainer2 = docker.containers().container("antidote2").port(8087);
        afs2 = new AntidoteFs(antidoteContainer2.inFormat("$HOST:$EXTERNAL_PORT"), refreshPeriod);
        rootDir2 = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs2, rootDir2);

        DockerPort antidoteContainer3 = docker.containers().container("antidote3").port(8087);
        afs3 = new AntidoteFs(antidoteContainer3.inFormat("$HOST:$EXTERNAL_PORT"), refreshPeriod);
        rootDir3 = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs3, rootDir3);
    }

    @AfterClass
    public static void unmountFs() {
        afs1.umount();
        afs2.umount();
        afs3.umount();
    }

    @Test
    public void basicFileCrud() throws Exception {
        String fileName = getRandomString();
        String content1 = getRandomString();
        String content2 = getRandomString();

        File file1 = new File(rootDir1.toAbsolutePath() + File.separator + fileName);
        assertFalse("file mustn't exist", file1.exists());
        assertTrue("file hasn't been created", file1.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file1)))) {
            writer.print(content1);
            writer.print(content2);
        }

        String txtRead1 = Files.lines(file1.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", content1 + content2, txtRead1);

        // read the file on the other mount point
        File fileOne2 = new File(rootDir2.toAbsolutePath() + File.separator + fileName);
        int i = 5, wait = refreshPeriod + propagationDelay; // XXX wait (i*wait) for propagation
                                                            // among fs local replicas
        while (!fileOne2.exists() && i > 0) {
            Thread.sleep(wait);
            i--;
        }
        assertTrue("file is not present on rootDir2", fileOne2.exists());

        String txtRead2 = Files.lines(fileOne2.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", txtRead1, txtRead2);
    }
}
