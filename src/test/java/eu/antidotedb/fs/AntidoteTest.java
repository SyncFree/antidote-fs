package eu.antidotedb.fs;

import static eu.antidotedb.client.Key.*;
import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;
import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.AntidoteStaticTransaction;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.Key;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.MapKey.MapReadResult;




/**
 * Temporary suite to test specific semantics of AntidoteDB.
 */
public class AntidoteTest {

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-antidote-single_host.yml")
            .waitingForService("antidote", HealthChecks.toHaveAllPortsOpen()).build();

    private static Bucket bucket;
    private static AntidoteClient antidote;

    @BeforeClass
    public static void setupAntidoteClient() {
        DockerPort antidoteContainer = docker.containers().container("antidote").port(8087);
        bucket = Bucket.bucket("test");
        String[] addrParts = antidoteContainer.inFormat("$HOST:$EXTERNAL_PORT").split(":");
        antidote = new AntidoteClient(new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1])));
    }
    
    @Ignore
    @Test
    public void readWriteInteger() throws Exception {
        MapKey rootmap = Key.map_aw("ROOT");
        AntidoteStaticTransaction tx = antidote.createStaticTransaction();
        bucket.update(tx, rootmap.update(
                integer("intreg").assign(10L)
                ));
        tx.commitTransaction();
        
        long res = (long) bucket.read(antidote.noTransaction(), rootmap).get(integer("intreg"));
        assertEquals(10L, res);
        long res1 = (long) bucket.read(antidote.noTransaction(), rootmap).get(integer("notexisting"));
        assertEquals(0L, res1);
    }
    
    
    // Support classes to experiment with recursive visit of file system.
    class MyFsElement {
        public String name;
        public MyFsElement parent = null;
    }
    class MyDir extends MyFsElement {
       public HashMap<String, MyFsElement> dir = new HashMap<String, MyFsElement>();
       public String toString() { return "<Dir " + name +">"; }
    }
    class MyFile extends MyFsElement {
        public byte[] content;
        public String toString() { return "<File " + name +">"; }
    }

    @Ignore
    @Test
    public void copyTreeTest() throws Exception {
        // create /ROOT/A/file1 /ROOT/A/B /ROOT/A/B/file2
        MapKey rootmap = Key.map_aw("ROOT");
        AntidoteStaticTransaction tx = antidote.createStaticTransaction();
        bucket.update(tx, rootmap.update(
                map_aw("A").update(
                        register("file1").assign("file1 content"),
                        map_aw("B").update(
                                register("file2").assign("file2 content")
                        ))
                ));
        tx.commitTransaction();
        
        // create /ROOT/C
        bucket.update(antidote.noTransaction(), 
                rootmap.update(map_aw("C").update(register("marker").assign(""))));
        
        MapReadResult res = bucket.read(antidote.noTransaction(), rootmap);
        assertTrue(res.keySet().contains(map_aw("A")));
        assertTrue(res.keySet().contains(map_aw("C")));
        
        MyDir rootdir = new MyDir();
        rootdir.name = "ROOT";
        visit(rootmap, rootdir);
        
        // print out content of rootdir
        printTree(rootdir, 0);
    }  
    
    private void printTree(MyDir dir, int nestLevel) {
        System.out.print(dir.name + "/ " + "\n");
        for(String key : dir.dir.keySet()) {
            MyFsElement el = dir.dir.get(key);
            if (el instanceof MyDir) {
                for (int i=0; i<=nestLevel; i++) 
                    System.out.print("\t");
                printTree((MyDir)el, nestLevel +1);
            } else {
                for (int i=0; i<=nestLevel; i++) 
                    System.out.print("\t");
                System.out.print(key + "\n");
            }
        }
    }
    
    private void visit(MapKey mapkey, MyDir dir) {
        MapReadResult res = bucket.read(antidote.noTransaction(), mapkey);
        recursiveVisit(res, dir);
    }
    private void recursiveVisit(MapReadResult res, MyDir dir) {
        for (Key<?> k : res.keySet()) {
            if (k.getType().equals(CRDT_type.AWMAP)) {
                MyDir subdir = new MyDir();
                subdir.name = k.getKey().toStringUtf8();
                subdir.parent = dir;
                dir.dir.put(subdir.name, subdir);
                //System.out.println("Adding " + subdir + " to " + dir);
                recursiveVisit(res.get((MapKey) k), subdir);
            } else if (k.getType().equals(CRDT_type.LWWREG)) {
                MyFile f = new MyFile();
                f.name = k.getKey().toStringUtf8();
                f.parent = dir;
                //String content = bucket.read(antidote.noTransaction(), ((MapKey) root)).get(register(k.getKey().toStringUtf8()));
                //String content = bucket.read(antidote.noTransaction(), map_aw(dir.parent.name)).get(((MapKey) root)).get(register(k.getKey().toStringUtf8()));
                //f.content = content.getBytes();
                dir.dir.put(f.name, f);
                //System.out.println("Adding " + f + " to " + dir);
            }
        }
    }
        
    /*
     * Tests that objects embedded on maps in Antidote are actually deleted, and not
     * just unlinked.
     */
    @Ignore
    @Test
    public void antidoteDeletionTest() throws Exception {
        // create /ROOT/A/file1 /ROOT/A/B
        MapKey rootmap = Key.map_aw("ROOT");
        AntidoteStaticTransaction tx = antidote.createStaticTransaction();
        bucket.update(tx, 
                rootmap.update(
                        map_aw("A").update(
                                register("file1").assign(""), 
                                map_aw("B").update(
                                        register("marker").assign("")
                                        )
                                )
                        ));
        tx.commitTransaction();
        
        // read /ROOT
        MapReadResult res = bucket.read(antidote.noTransaction(), rootmap);
        assertTrue(res.keySet().contains(map_aw("A")));
        assertFalse(res.keySet().contains(map_aw("file1")));
        
        // read /A
        MapReadResult res1 = bucket.read(antidote.noTransaction(), map_aw("A"));
        assertFalse(res1.keySet().contains(map_aw("file1")));
        assertTrue(res1.keySet().isEmpty());
        
        // remove /ROOT/A
        bucket.update(antidote.noTransaction(), rootmap.removeKey(map_aw("A")));
        res = bucket.read(antidote.noTransaction(), rootmap);
        assertFalse(res.keySet().contains(map_aw("A")));
        assertFalse(res.keySet().contains(register("file1")));

        // re-create /ROOT/A
        bucket.update(antidote.noTransaction(), rootmap.update(map_aw("A").update(register("marker").assign(""))));

        // check that /ROOT/A/file1 and /ROOT/A/B don't exist
        res = bucket.read(antidote.noTransaction(), rootmap).get(map_aw("A"));
        assertFalse(res.keySet().contains(map_aw("B")));
        assertFalse(res.keySet().contains(register("file1")));
    }
}
