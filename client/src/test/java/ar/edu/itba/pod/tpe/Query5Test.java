package ar.edu.itba.pod.tpe;

import ar.edu.itba.pod.tpe.client.queries.Query4;
import ar.edu.itba.pod.tpe.client.queries.Query5;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class Query5Test {
    private static final String TREE_NAME = "Arbol";
    private static final String STREET_NAME = "Calle Falsa";
    private static final double DIAMETER = 1.0;

    private static final String EXTRA_NEIGH = "Barrio Extra";

    private String[] neighbourhoods = {"Barrio A", "Barrio B", "Barrio C", "Barrio D"};
    private String[] neighbourhoods2 = {"Barrio A", "Barrio B", "Barrio C", "Barrio D", "Barrio E", "Barrio F"};

    private HazelcastInstance member, client;
    private JobTracker jobTracker;
    private IList<Tree> hzList;

    @Before
    public void setUp() {
        Config serverConfig = new Config();
        member = Hazelcast.newHazelcastInstance(serverConfig);
        ClientConfig clientConfig = new ClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        jobTracker = client.getJobTracker("g6-test-job-query5");
        hzList = client.getList("g6-test-list-query5");
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
        final SortedSet<ComparableTrio<Long, String, String>> result =
                Query5.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), jobTracker, client);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void testNoCouples() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        for (int i = 0; i < neighbourhoods.length; i++)             // each neighbourhood with i*1000 trees --> no couples
            for (int j = 0; j <= 1000*i; j++)
                list.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));

        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparableTrio<Long, String, String>> result =
                Query5.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), jobTracker, client);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void testTwoCouples() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        for (int j = 0; j <= 1200; j++)
            list.add(new Tree(neighbourhoods[0], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 1500; j++)
            list.add(new Tree(neighbourhoods[1], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 3200; j++)
            list.add(new Tree(neighbourhoods[2], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 3200; j++)
            list.add(new Tree(neighbourhoods[3], STREET_NAME, TREE_NAME, DIAMETER));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparableTrio<Long, String, String>> result =
                Query5.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), jobTracker, client);

        // Assertions
        assertEquals(2, result.size());
    }

    @Test
    public void testThreeCouples() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        for (int j = 0; j <= 1200; j++)
            list.add(new Tree(neighbourhoods2[0], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 1500; j++)
            list.add(new Tree(neighbourhoods2[1], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 3200; j++)
            list.add(new Tree(neighbourhoods2[2], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 3200; j++)
            list.add(new Tree(neighbourhoods2[3], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 5200; j++)
            list.add(new Tree(neighbourhoods2[4], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 5200; j++)
            list.add(new Tree(neighbourhoods2[5], STREET_NAME, TREE_NAME, DIAMETER));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparableTrio<Long, String, String>> result =
                Query5.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), jobTracker, client);

        // Assertions
        assertEquals(3, result.size());

        List<ComparableTrio<Long, String, String>> expected = Arrays.asList(
                new ComparableTrio<>(5000L, "Barrio E", "Barrio F"),
                new ComparableTrio<>(3000L, "Barrio C", "Barrio D"),
                new ComparableTrio<>(1000L, "Barrio A", "Barrio B"));

        int i = 0;
        for (ComparableTrio trio : result) {
            assertEquals(expected.get(i), trio);
            i++;
        }
    }

    @Test
    public void testFiveCouples() throws ExecutionException, InterruptedException {
        // Populate map
        List<Tree> list = new ArrayList<>();
        for (int j = 0; j <= 1200; j++)
            list.add(new Tree(neighbourhoods2[0], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 1500; j++)
            list.add(new Tree(neighbourhoods2[1], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 3200; j++)
            list.add(new Tree(neighbourhoods2[2], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 3200; j++)
            list.add(new Tree(neighbourhoods2[3], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 5200; j++)
            list.add(new Tree(neighbourhoods2[4], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 5200; j++)
            list.add(new Tree(neighbourhoods2[5], STREET_NAME, TREE_NAME, DIAMETER));
        for (int j = 0; j <= 1200; j++)
            list.add(new Tree(EXTRA_NEIGH, STREET_NAME, TREE_NAME, DIAMETER));
        hzList.addAll(list);

        // Run Query
        final SortedSet<ComparableTrio<Long, String, String>> result =
                Query5.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), jobTracker, client);

        // Assertions
        assertEquals(5, result.size());

        List<ComparableTrio<Long, String, String>> expected = Arrays.asList(
                new ComparableTrio<>(5000L, "Barrio E", "Barrio F"),
                new ComparableTrio<>(3000L, "Barrio C", "Barrio D"),
                new ComparableTrio<>(1000L, "Barrio A", "Barrio B"),
                new ComparableTrio<>(1000L, "Barrio A", EXTRA_NEIGH),
                new ComparableTrio<>(1000L, "Barrio B", EXTRA_NEIGH));

        int i = 0;
        for (ComparableTrio trio : result) {
            assertEquals(expected.get(i), trio);
            i++;
        }
    }
}
