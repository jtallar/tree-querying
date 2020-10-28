package ar.edu.itba.pod.tpe;

import ar.edu.itba.pod.tpe.client.queries.Query3;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class Query3Test {
    private static final String TREE_A = "Arbol A", TREE_B = "Arbol B", TREE_C = "Arbol C";
    private static final String STREET_NAME = "Calle Falsa";
    private static final double DIAMETER_A_1 = 5.0, DIAMETER_A_2 = 5.5;
    private static final double DIAMETER_B_1 = 3.0, DIAMETER_B_2 = 1.0;

    private HazelcastInstance member, client;
    private JobTracker jobTracker;
    private IList<Tree> hzList;

    @Before
    public void setUp() {
        Config serverConfig = new Config();
        member = Hazelcast.newHazelcastInstance(serverConfig);
        ClientConfig clientConfig = new ClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        jobTracker = client.getJobTracker("g6-test-job-query3");
        hzList = client.getList("g6-test-list-query3");
        hzList.clear();
    }

    @After
    public void finish() {
        client.shutdown();
        member.shutdown();
    }

    @Test
    public void testNoTrees() throws ExecutionException, InterruptedException {
        // Run Query
        final SortedSet<ComparablePair<Double, String>> result =
                Query3.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), 1);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void testDiametersOrder() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_1));
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_2));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_2));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparablePair<Double, String>> result =
                Query3.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), 2);
        List<ComparablePair<Double, String>> expected = Arrays.asList(
                new ComparablePair<>((DIAMETER_A_1+DIAMETER_A_2)/2, TREE_A),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_B));

        // Assertions
        int i = 0;
        for (ComparablePair<Double, String> pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testMoreDiametersOrder() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_1));
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_2));
        list.add(new Tree("10", STREET_NAME, TREE_C, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_C, DIAMETER_B_2));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_2));
        list.add(new Tree("10", STREET_NAME + "2", TREE_B + "2", DIAMETER_B_2+20.0));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparablePair<Double, String>> result =
                Query3.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), 4);
        List<ComparablePair<Double, String>> expected = Arrays.asList(
                new ComparablePair<>((DIAMETER_B_2+20.0), TREE_B + "2"),
                new ComparablePair<>((DIAMETER_A_1+DIAMETER_A_2)/2, TREE_A),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_B),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_C));

        // Assertions
        int i = 0;
        for (ComparablePair<Double, String> pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testDiametersTie() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_1));
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_2));
        list.add(new Tree("10", STREET_NAME, TREE_C, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_C, DIAMETER_B_2));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_2));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparablePair<Double, String>> result =
                Query3.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), 3);
        List<ComparablePair<Double, String>> expected = Arrays.asList(
                new ComparablePair<>((DIAMETER_A_1+DIAMETER_A_2)/2, TREE_A),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_B),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_C));

        // Assertions
        int i = 0;
        for (ComparablePair<Double, String> pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testListLimit() throws ExecutionException, InterruptedException {
        // Populate map
        final int limit = 1;
        List<Tree> list = new ArrayList<>();
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_1));
        list.add(new Tree("10", STREET_NAME, TREE_A, DIAMETER_A_2));
        list.add(new Tree("10", STREET_NAME, TREE_C, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_C, DIAMETER_B_2));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_1));
        list.add(new Tree("10", STREET_NAME, TREE_B, DIAMETER_B_2));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparablePair<Double, String>> result =
                Query3.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), limit);
        List<ComparablePair<Double, String>> expected = Arrays.asList(
                new ComparablePair<>((DIAMETER_A_1+DIAMETER_A_2)/2, TREE_A),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_B),
                new ComparablePair<>((DIAMETER_B_1+DIAMETER_B_2)/2, TREE_C));

        // Assertions
        assertEquals(limit, result.size());
    }
}
