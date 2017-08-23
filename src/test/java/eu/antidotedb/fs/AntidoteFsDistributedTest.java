package eu.antidotedb.fs;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.State;

public class AntidoteFsDistributedTest {

    private static String TEST_ROOT_DIR = "antidote-fs";

    private static AntidoteFs afs1;
    private static AntidoteFs afs2;
    private static AntidoteFs afs3;
    private static Path rootDir1;
    private static Path rootDir2;
    private static Path rootDir3;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-antidote-3dcs.yml").build();

    @BeforeClass
    public static void mountFs() throws IOException, InterruptedException {
        // wait for cluster setup (about 1.5s from here)
        while (!docker.containers().container("link").state().equals(State.HEALTHY)) 
            Thread.sleep(100);
        
        DockerPort antidoteContainer1 = docker.containers().container("antidote1").port(8087);
        afs1 = new AntidoteFs(antidoteContainer1.inFormat("$HOST:$EXTERNAL_PORT"));
        rootDir1 = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs1, rootDir1);
        
        DockerPort antidoteContainer2 = docker.containers().container("antidote2").port(8087);
        afs2 = new AntidoteFs(antidoteContainer2.inFormat("$HOST:$EXTERNAL_PORT"));
        rootDir2 = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs2, rootDir2);
        
        DockerPort antidoteContainer3 = docker.containers().container("antidote3").port(8087);
        afs3 = new AntidoteFs(antidoteContainer3.inFormat("$HOST:$EXTERNAL_PORT"));
        rootDir3 = Files.createTempDirectory(TEST_ROOT_DIR);
        blockingMount(afs3, rootDir3);
    }

    @AfterClass
    public static void unmountFs() {
        afs1.umount();
        afs2.umount();
        afs3.umount();
    }
    
    private static void blockingMount(AntidoteFs afs, Path rootDir) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            afs.mount(rootDir, false, true);
            latch.countDown();
        }).start();
        latch.await(1, TimeUnit.MINUTES);
    }
    
    @Test
    public void basicDistributedTest() throws Exception {
        File fileOne1 = new File(rootDir1.toAbsolutePath() + File.pathSeparator + "file1");
        assertFalse("file mustn't exist", fileOne1.exists());
        assertTrue("file hasn't been created", fileOne1.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileOne1)))) {
            writer.print("test1");
            writer.print("test2");
        }       
        
        // XXX WIP
        File fileOne2 = new File(rootDir2.toAbsolutePath() + File.pathSeparator + "file1");
        long s = System.currentTimeMillis();
        while (!fileOne2.exists())
            Thread.sleep(200);
        long e = System.currentTimeMillis();
        System.out.println("TIME: " + (s-e));
        assertTrue("file is not present on rootDir2", fileOne2.exists());
    }

    @Test
    public void basicFileCrudTest() throws Exception {
        File fileOne = new File(rootDir1.toAbsolutePath() + File.pathSeparator + "file1");
        assertFalse("file mustn't exist", fileOne.exists());
        assertTrue("file hasn't been created", fileOne.createNewFile());
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileOne)))) {
            writer.print("test1");
            writer.print("test2");
        }

        String text = Files.lines(fileOne.toPath()).collect(Collectors.joining());
        assertEquals("file content doesn't match what was written", "test1test2", text);

        assertTrue("file can't be deleted", fileOne.delete());
        assertFalse("file mustn't exist", fileOne.exists());
    }
}
