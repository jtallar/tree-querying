package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;
import ar.edu.itba.pod.tpe.keyPredicates.TestKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.TestMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.TestReducer;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TestClient {
    private static Logger logger = LoggerFactory.getLogger(TestClient.class);

    private static final String ADDRESS_PARAM = "addresses";
    private static final int ERROR_STATUS = 1;

    private static List<String> clusterAddresses = new ArrayList<>();

    public static void main(String[] args) {
        logger.info("tpe2-g6 TestClient Starting ...");

        try {
            argumentParsing();
        } catch (ArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }
//
//        try {
//            long a, b;
//            a = System.currentTimeMillis();
//            logger.info("Arranca listado " + a);
//            Parser.parseTrees("/home/julian/Desktop/POD/tpe2-g6/test-files/", City.of("BUE"));
//            b = System.currentTimeMillis();
//            logger.info("Termina listado " + (b - a));
//
//            a = System.currentTimeMillis();
//            logger.info("Arranca mapeado " + a);
//            Parser.parseTreesMap("/home/julian/Desktop/POD/tpe2-g6/test-files/", City.of("BUE"));
//            b = System.currentTimeMillis();
//            logger.info("Termina mapeado " + (b - a));
//        } catch (IOException e) {
//            System.err.println(e.getMessage());
//            System.exit(ERROR_STATUS);
//            return;
//        }

        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("g6-cluster").setPassword("123456");
        config.getNetworkConfig().addAddress(clusterAddresses.toArray(new String[0]));
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        JobTracker jobTracker = hz.getJobTracker("g6-test-query");

        final IMap<Neighbourhood, List<Tree>> map = hz.getMap("g6-test-query");
        map.clear();
        try {
            map.putAll(Parser.parseTrees("/home/julian/Desktop/POD/tpe2-g6/test-files/", City.of("BUE")));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }

        Map<String, Integer> neigh;
        try {
             neigh = Parser.parseNeighbourhood("/home/julian/Desktop/POD/tpe2-g6/test-files/", City.of("BUE"));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }

        final KeyValueSource<Neighbourhood, List<Tree>> source = KeyValueSource.fromMap(map);
        final Job<Neighbourhood, List<Tree>> job = jobTracker.newJob(source);
        final JobCompletableFuture<Map<Neighbourhood, Long>> future = job
                .keyPredicate(new TestKeyPredicate(neigh))
                .mapper(new TestMapper())
//                .combiner(new WordCountCombinerFactory())
                .reducer(new TestReducer())
                .submit();

        // Wait and retrieve result
        Map<Neighbourhood, Long> result = new HashMap<>();
        try {
             result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(result);

        hz.shutdown();
    }

    private static void argumentParsing() throws ArgumentException {
        Properties properties = System.getProperties();

        String[] clusterNodes = Optional.ofNullable(properties.getProperty(ADDRESS_PARAM)).orElse("").split(",");
        if (clusterNodes.length == 0) {
            throw new ArgumentException("No addresses provided.\nNode addresses must be supplied using -Daddresses and its format must be " +
                    "xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY");
        }

        for (String node : clusterNodes) {
            InetSocketAddress inetAddress;
            try {
                inetAddress = ClientUtils.getInetAddress(node);
            } catch (URISyntaxException e) {
                throw new ArgumentException("Invalid address.\nNode addresses must be supplied using -Daddresses and its format must be " +
                        "xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY");
            }
            clusterAddresses.add(node);
        }
    }
}
