package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.collators.Query2Collator;
import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;
import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.Parser;
import ar.edu.itba.pod.tpe.keyPredicates.NeighbourhoodKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.Query2Mapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query2Reducer;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
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

public class Query2Client {
    private static Logger logger = LoggerFactory.getLogger(Query2Client.class);

    private static final String ADDRESS_PARAM = "addresses";
    private static final int ERROR_STATUS = 1;

    private static List<String> clusterAddresses = new ArrayList<>();

    public static void main(String[] args) {
        logger.info("tpe2-g6 Query2 Starting ...");

        try {
            argumentParsing();
        } catch (ArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }

        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("g6-cluster").setPassword("123456");
        config.getNetworkConfig().addAddress(clusterAddresses.toArray(new String[0]));

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);
        JobTracker jobTracker = hz.getJobTracker("tree-street");


        Map<String, Integer>  neigh;
        try {
            neigh = Parser.parseNeighbourhood("/Users/nicolas/Downloads/csvs_POD/", City.of("BUE"));
        } catch (IOException | ArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }


        final IMap<Neighbourhood, List<Tree>> hzTree = hz.getMap("g6-query2");
        hzTree.clear();
        try {
            hzTree.putAll(Parser.parseTrees("/Users/nicolas/Downloads/csvs_POD/", City.of("BUE")));
        } catch (IOException | ArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }


        final KeyValueSource<Neighbourhood, List<Tree>> source = KeyValueSource.fromMap(hzTree);
        final Job<Neighbourhood, List<Tree>> job = jobTracker.newJob(source);


        logger.info("Inicio del map/reduce");
        final JobCompletableFuture<Map<String,ComparablePair<String,Long>>> future = job
                .keyPredicate(new NeighbourhoodKeyPredicate(neigh))
                .mapper(new Query2Mapper())
                .reducer(new Query2Reducer())
                .submit(new Query2Collator(2000));

        // Wait and retrieve result
        Map<String,ComparablePair<String,Long>> result = new HashMap<>();

        try {
            result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        logger.info("Fin del map/reduce");

        for(String s : result.keySet()){
            result.get(s);
            System.out.println("BARRIO: " + s + " ARBOL: " + result.get(s).getFirst() + " CANT: " + result.get(s).getSecond());
        }


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
