package ar.edu.itba.pod.tpe;

import ar.edu.itba.pod.tpe.client.queries.Query2;
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

public class Query2Test {
    private static final String TREE_NAME = "Arbol";
    private static final String STREET_NAME = "Calle Falsa";
    private static final double DIAMETER = 1.0;

    private static final Long POPULATION = 1000L;

    private String[] neighbourhoods = {"10", "12", "13", "4", "9"};
    private static final String OTHER_NEIGHBOURHOOD = "BARRIO RANDOM";
    private static final int TREES_QTY = 3;


    private HazelcastInstance member, client;
    private JobTracker jobTracker;
    private IList<Tree> hzList;

    @Before
    public void setUp() {
        Config serverConfig = new Config();
        member = Hazelcast.newHazelcastInstance(serverConfig);
        ClientConfig clientConfig = new ClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        jobTracker = client.getJobTracker("g6-test-job-query2");
        hzList = client.getList("g6-test-list-query2");

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
        final Map<String,ComparablePair<String,Long>> result =
                Query2.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh, 1);

        // Assertions
        assertEquals(0, result.size());
    }


    @Test
    public void treesWithoutNeighbourhood() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods){
            neigh.put(neighbourhood,POPULATION);
        }

        List<Tree> trees = new ArrayList<>();
        for (int i = 0; i < TREES_QTY ; i++) {
            trees.add(new Tree(OTHER_NEIGHBOURHOOD, STREET_NAME, TREE_NAME, DIAMETER));
        }
        hzList.addAll(trees);

        // Run Query
        final Map<String,ComparablePair<String,Long>> result =
                Query2.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh, 1);

        // Assertions
        assertEquals(0, result.size());
    }

    @Test
    public void treesWithNeighbourhood() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods){
            neigh.put(neighbourhood,POPULATION);
        }

        List<Tree> trees = new ArrayList<>();
        for (int i = 0; i < TREES_QTY ; i++) {
            trees.add(new Tree(neighbourhoods[0], STREET_NAME, TREE_NAME, DIAMETER));
        }
        hzList.addAll(trees);

        // Run Query
        final Map<String,ComparablePair<String,Long>> result =
                Query2.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh, 1);

        // Assertions
        assertEquals(1, result.size());
    }


    @Test
    public void treesWithNeighbourhood2() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods){
            neigh.put(neighbourhood,POPULATION);
        }

        List<Tree> trees = new ArrayList<>();
        for (int i = 0; i < TREES_QTY ; i++) {
            trees.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));
        }
        hzList.addAll(trees);

        // Run Query
        final Map<String,ComparablePair<String,Long>> result =
                Query2.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh, 1);

        // Assertions
        assertEquals(TREES_QTY, result.size());
    }

    @Test
    public void minTooBig() throws ExecutionException, InterruptedException{
        Map<String, Long> neigh = new HashMap<>();

        // Populate map's
        for(String neighbourhood : neighbourhoods){
            neigh.put(neighbourhood,POPULATION);
        }

        List<Tree> trees = new ArrayList<>();
        for (int i = 0; i < TREES_QTY ; i++) {
            trees.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));
            trees.add(new Tree(neighbourhoods[i], STREET_NAME, TREE_NAME, DIAMETER));
        }
        hzList.addAll(trees);

        // Run Query
        final Map<String,ComparablePair<String,Long>> result =
                Query2.runQuery(jobTracker.newJob(KeyValueSource.fromList(hzList)), neigh, 4);

        // Assertions
        assertEquals(0, result.size());
    }

}
