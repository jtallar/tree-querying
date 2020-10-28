package ar.edu.itba.pod.tpe;

import ar.edu.itba.pod.tpe.client.queries.Query1;
import ar.edu.itba.pod.tpe.client.queries.Query2;
import ar.edu.itba.pod.tpe.client.queries.Query4;
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

public class Query1Test {
    private static final String STREET_NAME = "Calle Falsa";
    private static final double DIAMETER = 1.0;
    private static final Long POPULATION_A = 1000L, POPULATION_B= 2000L, POPULATION_C = 500L;
    private static final String TREE_NAME = "Arbol 1", OTHER_TREE_NAME = "Arbol 2", THIRD_TREE_NAME = "Arbol 3";

    private final String[] neighbourhoods1 = {"Barrio A", "Barrio B", "Barrio C"}, neighbourhoods2 = {"Barrio D", "Barrio E"};

    private HazelcastInstance member, client;
    private JobTracker jobTracker;
    private IList<Tree> hzList;

    @Before
    public void setUp() {
        Config serverConfig = new Config();
        member = Hazelcast.newHazelcastInstance(serverConfig);
        ClientConfig clientConfig = new ClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        jobTracker = client.getJobTracker("g6-test-job-query1");
        hzList = client.getList("g6-test-list-query1");
        hzList.clear();
    }

    @After
    public void finish() {
        client.shutdown();
        member.shutdown();
    }

    @Test
    public void testNoTrees() throws ExecutionException, InterruptedException {
        Map<String, Long> neigh = new HashMap<>();

        // Run Query
        final Set<ComparablePair<Double, String>> result =
                Query1.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void treesWithoutNeighbourhood() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods1)
            neigh.put(neighbourhood,POPULATION_A);
        for(String neighbourhood : neighbourhoods2)
            neigh.put(neighbourhood,POPULATION_B);

        List<Tree> trees = new ArrayList<>();
        for (int i = 0; i < 3 ; i++)
            trees.add(new Tree("OTHER_NEIGHBOURHOOD", STREET_NAME, TREE_NAME, DIAMETER));

        hzList.addAll(trees);

        // Run Query
        final Set<ComparablePair<Double, String>> result =
                Query1.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void treesWithNeighbourhood() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods1)
            neigh.put(neighbourhood,POPULATION_A);
        for(String neighbourhood : neighbourhoods2)
            neigh.put(neighbourhood,POPULATION_B);

        List<Tree> trees = new ArrayList<>();
        for (int i = 0; i < 3 ; i++)
            trees.add(new Tree(neighbourhoods1[0], STREET_NAME, TREE_NAME, DIAMETER));

        hzList.addAll(trees);

        // Run Query
        final Set<ComparablePair<Double, String>> result =
                Query1.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh);

        // Assertions
        assertEquals(1, result.size());
    }

    @Test
    public void treesPerHab() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods1)
            neigh.put(neighbourhood, POPULATION_A);             // neigh from neigh1 -> 1000L pop
        for(String neighbourhood : neighbourhoods2)
            neigh.put(neighbourhood, POPULATION_B);              // neigh from neigh2 -> 2000L pop

        List<Tree> trees = new ArrayList<>();
        for(String neighbourhood : neighbourhoods1) {                    // 3 trees by neigh
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
        }
        for(String neighbourhood : neighbourhoods2) {                  // 3 trees by neigh
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
        }
        hzList.addAll(trees);

        // build expected result
        List<ComparablePair<Double, String>> expected = Arrays.asList(
                new ComparablePair<>(3/(double) POPULATION_A, neighbourhoods1[0]),
                new ComparablePair<>(3/(double) POPULATION_A, neighbourhoods1[1]),
                new ComparablePair<>(3/(double) POPULATION_A, neighbourhoods1[2]),
                new ComparablePair<>(3/(double) POPULATION_B, neighbourhoods2[0]),
                new ComparablePair<>(3/(double) POPULATION_B, neighbourhoods2[1]));

        // Run Query
        final Set<ComparablePair<Double, String>> result =
                Query1.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh);

        int i = 0;
        for (ComparablePair pair : result)
            assertEquals(expected.get(i++), pair);

        // Assertions
        assertEquals(expected.size(), result.size());
    }

    @Test
    public void anotherTreesPerHab() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods1)
            neigh.put(neighbourhood, POPULATION_A);             // neigh from neigh1 -> 1000L pop
        for(String neighbourhood : neighbourhoods2)
            neigh.put(neighbourhood, POPULATION_C);              // neigh from neigh2 -> 500L pop

        List<Tree> trees = new ArrayList<>();
        for(String neighbourhood : neighbourhoods1) {                    // 3 trees by neigh
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
        }
        for(String neighbourhood : neighbourhoods2) {                  // 3 trees by neigh
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhood, STREET_NAME, TREE_NAME, DIAMETER));
        }
        hzList.addAll(trees);

        // build expected result
        List<ComparablePair<Double, String>> expected = Arrays.asList(
                new ComparablePair<>(3/(double) POPULATION_C, neighbourhoods2[0]),
                new ComparablePair<>(3/(double) POPULATION_C, neighbourhoods2[1]),
                new ComparablePair<>(3/(double) POPULATION_A, neighbourhoods1[0]),
                new ComparablePair<>(3/(double) POPULATION_A, neighbourhoods1[1]),
                new ComparablePair<>(3/(double) POPULATION_A, neighbourhoods1[2]));

        // Run Query
        final Set<ComparablePair<Double, String>> result =
                Query1.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh);

        int i = 0;
        for (ComparablePair pair : result)
            assertEquals(expected.get(i++), pair);

        // Assertions
        assertEquals(expected.size(), result.size());
    }
}
