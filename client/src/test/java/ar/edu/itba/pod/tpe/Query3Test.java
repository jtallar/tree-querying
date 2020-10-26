package ar.edu.itba.pod.tpe;

import ar.edu.itba.pod.tpe.client.queries.Query3;
import ar.edu.itba.pod.tpe.client.queries.Query4;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
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
    private IMap<Neighbourhood, List<Tree>> hzMap;

    @Before
    public void setUp() {
        Config serverConfig = new Config();
        member = Hazelcast.newHazelcastInstance(serverConfig);
        ClientConfig clientConfig = new ClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        jobTracker = client.getJobTracker("g6-test-job-query3");
        hzMap = client.getMap("g6-test-map-query3");
        hzMap.clear();
    }

    @After
    public void finish() {
        client.shutdown();
        member.shutdown();
    }

    @Test
    public void testNoTrees() throws ExecutionException, InterruptedException {
        // Run Query
        final SortedSet<ComparablePair<String, Double>> result =
                Query3.runQueryTest(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), 1);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void testDiametersOrder() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        Neighbourhood n = new Neighbourhood("10");
        map.put(n, new ArrayList<>());
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_2));
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, Double>> result =
                Query3.runQueryTest(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), 2);
        List<ComparablePair<String, Double>> expected = Arrays.asList(
                new ComparablePair<>(TREE_A, (DIAMETER_A_1+DIAMETER_A_2)/2),
                new ComparablePair<>(TREE_B, (DIAMETER_B_1+DIAMETER_B_2)/2));

        // Assertions
        int i = 0;
        for (ComparablePair<String, Double> pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testMoreDiametersOrder() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        Neighbourhood n = new Neighbourhood("10");
        map.put(n, new ArrayList<>());
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_C, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_C, DIAMETER_B_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_2));
        map.get(n).add(new Tree(STREET_NAME + "2", TREE_B + "2", DIAMETER_B_2+20.0));
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, Double>> result =
                Query3.runQueryTest(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), 4);
        List<ComparablePair<String, Double>> expected = Arrays.asList(
                new ComparablePair<>(TREE_B + "2", (DIAMETER_B_2+20.0)),
                new ComparablePair<>(TREE_A, (DIAMETER_A_1+DIAMETER_A_2)/2),
                new ComparablePair<>(TREE_B, (DIAMETER_B_1+DIAMETER_B_2)/2),
                new ComparablePair<>(TREE_C, (DIAMETER_B_1+DIAMETER_B_2)/2));

        // Assertions
        int i = 0;
        for (ComparablePair<String, Double> pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testDiametersTie() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        Neighbourhood n = new Neighbourhood("10");
        map.put(n, new ArrayList<>());
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_C, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_C, DIAMETER_B_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_2));
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, Double>> result =
                Query3.runQueryTest(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), 3);
        List<ComparablePair<String, Double>> expected = Arrays.asList(
                new ComparablePair<>(TREE_A, (DIAMETER_A_1+DIAMETER_A_2)/2),
                new ComparablePair<>(TREE_B, (DIAMETER_B_1+DIAMETER_B_2)/2),
                new ComparablePair<>(TREE_C, (DIAMETER_B_1+DIAMETER_B_2)/2));

        // Assertions
        int i = 0;
        for (ComparablePair<String, Double> pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testListLimit() throws ExecutionException, InterruptedException {
        // Populate map
        final int limit = 1;
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        Neighbourhood n = new Neighbourhood("10");
        map.put(n, new ArrayList<>());
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_A, DIAMETER_A_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_C, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_C, DIAMETER_B_2));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_1));
        map.get(n).add(new Tree(STREET_NAME, TREE_B, DIAMETER_B_2));
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, Double>> result =
                Query3.runQueryTest(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), limit);
        List<ComparablePair<String, Double>> expected = Arrays.asList(
                new ComparablePair<>(TREE_A, (DIAMETER_A_1+DIAMETER_A_2)/2),
                new ComparablePair<>(TREE_B, (DIAMETER_B_1+DIAMETER_B_2)/2),
                new ComparablePair<>(TREE_C, (DIAMETER_B_1+DIAMETER_B_2)/2));

        // Assertions
        assertEquals(limit, result.size());
    }
}
