package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.client.ClientUtils;
import ar.edu.itba.pod.tpe.client.TestClient;
import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;
import ar.edu.itba.pod.tpe.mappers.NeighbourhoodKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.TestMapper;
import ar.edu.itba.pod.tpe.mappers.TreeStreetMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.models.TreeStreet;
import ar.edu.itba.pod.tpe.reducers.TestReducer;
import ar.edu.itba.pod.tpe.reducers.TreeStreetReducer;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
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

    public static void main(String[] args) throws IOException {
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


        List<Neighbourhood> neig = Parser.parseNeighbourhood("/Users/nicolas/Downloads/csvs_POD/barriosBUE.csv");
        List<Tree> trees = Parser.parseBUETrees("/Users/nicolas/Downloads/csvs_POD/arbolesBUE.csv");


        final IList<Neighbourhood> neighbourhoodIList = hz.getList("neigbourhoods");
        final IList<Tree> treeIList = hz.getList("tree");

        System.out.println(trees.get(0).toString());
        System.out.println(neig.get(0).toString());

        neighbourhoodIList.clear();
        treeIList.clear();
        neighbourhoodIList.addAll(neig);
        treeIList.addAll(trees);

        System.out.println(treeIList.size());
        System.out.println(neighbourhoodIList.size());

        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(treeIList);
        final Job<String, Tree> job = jobTracker.newJob(source);

/*        System.out.println(treeIList.get(0).toString());
        System.out.println(neighbourhoodIList.get(0).toString());*/

        logger.info("Inicio del map/reduce");
        final JobCompletableFuture<Map<TreeStreet, Long>> future = job
        //        .keyPredicate(new NeighbourhoodKeyPredicate(neighbourhoodIList))
                .mapper(new TreeStreetMapper())
                .reducer(new TreeStreetReducer())
                .submit();

        // Wait and retrieve result
        Map<TreeStreet, Long> result = new HashMap<>();

        System.out.println(result.size());
        try {
            result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        logger.info("Fin del map/reduce");
        for(TreeStreet t : result.keySet()){
            System.out.println(t.getNeighbourhood() + ' ' + t.getTree() + ' ' + result.get(t));
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
