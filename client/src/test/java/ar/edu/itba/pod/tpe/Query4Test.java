package ar.edu.itba.pod.tpe;

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
import static org.junit.Assert.assertEquals;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query4Test {
    private static final String TREE_NAME = "Arbol";
    private static final String STREET_NAME = "Calle Falsa";
    private static final double DIAMETER = 1.0;

    private static final String OTHER_TREE_NAME = "Arbol 2";
    private static final String THIRD_TREE_NAME = "Arbol 3";

    private String[] neighbourhoods = {"10", "12", "13", "4", "9"};

    private HazelcastInstance member, client;
    private JobTracker jobTracker;
    private IMap<Neighbourhood, List<Tree>> hzMap;

    @Before
    public void setUp() {
        Config serverConfig = new Config();
        member = Hazelcast.newHazelcastInstance(serverConfig);
        ClientConfig clientConfig = new ClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        jobTracker = client.getJobTracker("g6-test-job-query4");
        hzMap = client.getMap("g6-test-map-query4");
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
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), TREE_NAME, 1);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void testOtherTreeName() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        for (int i = 0; i < neighbourhoods.length; i++) {
            Neighbourhood n = new Neighbourhood(neighbourhoods[i]);
            map.put(n, new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                map.get(n).add(new Tree(STREET_NAME, TREE_NAME, DIAMETER));
                map.get(n).add(new Tree(STREET_NAME, OTHER_TREE_NAME, DIAMETER));
            }
        }
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), THIRD_TREE_NAME, 1);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void testAllNeighbourhoodsMin1() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        for (int i = 0; i < neighbourhoods.length; i++) {
            Neighbourhood n = new Neighbourhood(neighbourhoods[i]);
            map.put(n, new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                map.get(n).add(new Tree(STREET_NAME, TREE_NAME, DIAMETER));
                map.get(n).add(new Tree(STREET_NAME, OTHER_TREE_NAME, DIAMETER));
            }
        }
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), TREE_NAME, 1);

        // Assertions
        List<ComparablePair<String, String>> expected = Arrays.asList(
                new ComparablePair<>("10", "12"),
                new ComparablePair<>("10", "13"),
                new ComparablePair<>("10", "4"),
                new ComparablePair<>("10", "9"),
                new ComparablePair<>("12", "13"),
                new ComparablePair<>("12", "4"),
                new ComparablePair<>("12", "9"),
                new ComparablePair<>("13", "4"),
                new ComparablePair<>("13", "9"),
                new ComparablePair<>("4", "9"));

        int i = 0;
        for (ComparablePair pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testAllNeighbourhoodsMin2() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        for (int i = 0; i < neighbourhoods.length; i++) {
            Neighbourhood n = new Neighbourhood(neighbourhoods[i]);
            map.put(n, new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                map.get(n).add(new Tree(STREET_NAME, TREE_NAME, DIAMETER));
                map.get(n).add(new Tree(STREET_NAME, OTHER_TREE_NAME, DIAMETER));
            }
        }
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), TREE_NAME, 2);

        // Assertions
        List<ComparablePair<String, String>> expected = Arrays.asList(
                new ComparablePair<>("12", "13"),
                new ComparablePair<>("12", "4"),
                new ComparablePair<>("12", "9"),
                new ComparablePair<>("13", "4"),
                new ComparablePair<>("13", "9"),
                new ComparablePair<>("4", "9"));

        int i = 0;
        for (ComparablePair pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testAllNeighbourhoodsMin3() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        for (int i = 0; i < neighbourhoods.length; i++) {
            Neighbourhood n = new Neighbourhood(neighbourhoods[i]);
            map.put(n, new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                map.get(n).add(new Tree(STREET_NAME, TREE_NAME, DIAMETER));
                map.get(n).add(new Tree(STREET_NAME, OTHER_TREE_NAME, DIAMETER));
            }
        }
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), TREE_NAME, 3);

        // Assertions
        List<ComparablePair<String, String>> expected = Arrays.asList(
                new ComparablePair<>("13", "4"),
                new ComparablePair<>("13", "9"),
                new ComparablePair<>("4", "9"));

        int i = 0;
        for (ComparablePair pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testAllNeighbourhoodsMin4() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        for (int i = 0; i < neighbourhoods.length; i++) {
            Neighbourhood n = new Neighbourhood(neighbourhoods[i]);
            map.put(n, new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                map.get(n).add(new Tree(STREET_NAME, TREE_NAME, DIAMETER));
                map.get(n).add(new Tree(STREET_NAME, OTHER_TREE_NAME, DIAMETER));
            }
        }
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), TREE_NAME, 4);

        // Assertions
        List<ComparablePair<String, String>> expected = Collections.singletonList(
                new ComparablePair<>("4", "9"));

        int i = 0;
        for (ComparablePair pair : result) {
            assertEquals(expected.get(i), pair);
            i++;
        }
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void testAllNeighbourhoodsMin5() throws ExecutionException, InterruptedException {
        // Populate map
        Map<Neighbourhood, List<Tree>> map = new HashMap<>();
        for (int i = 0; i < neighbourhoods.length; i++) {
            Neighbourhood n = new Neighbourhood(neighbourhoods[i]);
            map.put(n, new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                map.get(n).add(new Tree(STREET_NAME, TREE_NAME, DIAMETER));
                map.get(n).add(new Tree(STREET_NAME, OTHER_TREE_NAME, DIAMETER));
            }
        }
        hzMap.putAll(map);

        // Run Query
        final SortedSet<ComparablePair<String, String>> result =
                Query4.runQuery(jobTracker.newJob(KeyValueSource.fromMap(hzMap)), TREE_NAME, 5);

        // Assertions
        assertEquals(0, result.size());
    }
}
