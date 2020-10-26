package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;
import ar.edu.itba.pod.tpe.client.queries.Query1;
import ar.edu.itba.pod.tpe.client.queries.Query4;
import ar.edu.itba.pod.tpe.client.queries.Query5;
import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.Parser;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Query;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private static final String QUERY_NAME_PARAM = "queryName";
    private static final String CITY_PARAM = "city";
    private static final String ADDRESS_PARAM = "addresses";
    private static final String IN_PATH_PARAM = "inPath";
    private static final String OUT_PATH_PARAM = "outPath";

    private static final String MIN_PARAM = "min";
    private static final String NAME_PARAM = "name";
    private static final String N_PARAM = "n";

    private static final int ERROR_STATUS = 1;

    private static String query;
    private static City city;
    private static List<String> clusterAddresses = new ArrayList<>();
    private static String inPath, outPath;
    private static int minNumber;
    private static String treeName;
    private static Map<String, Long>  neighborhoods;

    public static void main(String[] args) {
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

        JobTracker jobTracker = hz.getJobTracker("g6-job-" + query);

        final IMap<Neighbourhood, List<Tree>> map = hz.getMap("g6-map-" + query);
        map.clear();
        logger.info("Inicio de la lectura del archivo");
        try {
            map.putAll(Parser.parseTrees(inPath, city));
        } catch (IOException e) {
            System.err.println("Could not read trees file from directory " + inPath);
            System.exit(ERROR_STATUS);
            return;
        }
        logger.info("Fin de la lectura del archivo");


        logger.info("Inicio de la lectura del archivo de ciudades");
        try {
            neighborhoods = Parser.parseNeighbourhood(inPath, city);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }
        logger.info("Fin de la lectura del archivo de ciudades");


        final KeyValueSource<Neighbourhood, List<Tree>> source = KeyValueSource.fromMap(map);
        final Job<Neighbourhood, List<Tree>> job = jobTracker.newJob(source);

        logger.info("Inicio del trabajo map/reduce");
        try {
            runQuery(job, jobTracker);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Future execution interrupted. Exiting...");
            System.exit(ERROR_STATUS);
            return;
        }
        logger.info("Fin del trabajo map/reduce");

        hz.shutdown();
    }

    // TODO: ver como marcar que argumentos son requeridos segun el numero de query
    private static void argumentParsing() throws ArgumentException {
        Properties properties = System.getProperties();

        query = Optional.ofNullable(properties.getProperty(QUERY_NAME_PARAM)).orElseThrow(new ArgumentException("Programmer Error! Query name must be supplied in .sh!"));

        city = City.of(Optional.ofNullable(properties.getProperty(CITY_PARAM)).orElseThrow(new ArgumentException("City must be supplied using -Dcity")));

        String[] clusterNodes = Optional.ofNullable(properties.getProperty(ADDRESS_PARAM)).orElse("").split(",");
        if (clusterNodes.length == 0) {
            throw new ArgumentException("No addresses provided.\nNode addresses must be supplied using -Daddresses and its format must be " +
                    "xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY");
        }
        for (String node : clusterNodes) {
            try {
                ClientUtils.getInetAddress(node);
            } catch (URISyntaxException e) {
                throw new ArgumentException("Invalid address.\nNode addresses must be supplied using -Daddresses and its format must be " +
                        "xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY");
            }
            clusterAddresses.add(node);
        }

        inPath = Optional.ofNullable(properties.getProperty(IN_PATH_PARAM)).orElseThrow(new ArgumentException("In path must be supplied using -DinPath"));
        outPath = Optional.ofNullable(properties.getProperty(OUT_PATH_PARAM)).orElseThrow(new ArgumentException("Out path must be supplied using -DoutPath"));

        try {
            minNumber = Integer.parseInt(properties.getProperty(MIN_PARAM));
            if (minNumber < 0) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            throw new ArgumentException("min number must be supplied using -Dmin and it must be a positive or zero number");
        }

        treeName = Optional.ofNullable(properties.getProperty(NAME_PARAM)).orElseThrow(new ArgumentException("Tree name must be supplied using -Dname"));
    }

    // TODO: VER COMO HACER ESTO DE MANERA MAS PROLIJA
    private static void runQuery(Job<Neighbourhood, List<Tree>> job, JobTracker jobTracker)
            throws InterruptedException, ExecutionException {

        switch (query) {
            case "query1":
                Query1.runQuery(job, neighborhoods, outPath);
                break;
            case "query2":
                break;
            case "query3":
                break;
            case "query4":
                Query4.runQuery(job, treeName, minNumber, outPath);
                break;
            case "query5":
                Query5.runQuery(job, jobTracker, outPath);
                break;
            default:
                break;
        }
    }
}