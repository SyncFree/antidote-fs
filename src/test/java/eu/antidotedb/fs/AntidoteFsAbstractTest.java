package eu.antidotedb.fs;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class AntidoteFsAbstractTest {

    protected static final Random random = new Random();

    protected static void blockingMount(AntidoteFs afs, Path rootDir) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            afs.mount(rootDir, false, true);
            latch.countDown();
        }).start();
        latch.await(1, TimeUnit.MINUTES);
    }

    protected static String getRandomString() {
        return new BigInteger(50, random).toString(32);
    }
}
