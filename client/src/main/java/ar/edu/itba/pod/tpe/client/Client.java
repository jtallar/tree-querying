package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;
import ar.edu.itba.pod.tpe.client.queries.Query2;
import ar.edu.itba.pod.tpe.client.queries.Query1;
import ar.edu.itba.pod.tpe.client.queries.Query3;
import ar.edu.itba.pod.tpe.client.queries.Query4;
import ar.edu.itba.pod.tpe.client.queries.Query5;
import ar.edu.itba.pod.tpe.client.utils.City;
import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.Parser;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final int MIN_PARAM_INDEX = 0;
    private static final int NAME_PARAM_INDEX = 1;
    private static final int N_PARAM_INDEX = 2;

    private static final int ERROR_STATUS = 1;

    private static String query;
    private static City city;
    private static List<String> clusterAddresses = new ArrayList<>();
    private static String inPath, outPath;
    private static long minNumber, limit;
    private static String treeName;

    public static void main(String[] args) {
        try {
            argumentParsing();
        } catch (ArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(ERROR_STATUS);
            return;
        }

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(getClientConfig());

        JobTracker jobTracker = hz.getJobTracker("g6-job-" + query);

        final IList<Tree> list = hz.getList("g6-list-" + query);
        list.clear();
        logger.info("Inicio de la lectura del archivo arboles");
        System.out.println("Iniciando lectura del archivo de arboles...");
        try {
            list.addAll(Parser.parseTrees(inPath, city));
        } catch (IOException e) {
            System.err.println("Could not read trees file from directory " + inPath);
            System.exit(ERROR_STATUS);
            return;
        }
        logger.info("Fin de la lectura del archivo arboles");

        System.out.println("Iniciando trabajo de map/reduce para la " + query + "...");
        logger.info("Inicio del trabajo map/reduce");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(list);
        final Job<String, Tree> job = jobTracker.newJob(source);
        try {
            runQuery(job, jobTracker, hz);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Future execution interrupted. Exiting...");
            System.exit(ERROR_STATUS);
            return;
        } catch (IOException e) {
            System.err.println("Could not read neighbourhoods file from directory " + inPath);
            System.exit(ERROR_STATUS);
            return;
        }
        logger.info("Fin del trabajo map/reduce");

        hz.shutdown();
        System.out.println(query + " terminada, saliendo...");
    }

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

        final boolean[] additionalParams = getAdditionalParams();

        if (additionalParams[MIN_PARAM_INDEX]) {
            try {
                minNumber = Long.parseLong(properties.getProperty(MIN_PARAM));
                if (minNumber <= 0) throw new NumberFormatException(); // minNumber debe ser un entero positivo
            } catch (NumberFormatException e) {
                throw new ArgumentException("min number must be supplied using -Dmin and it must be a positive number (min > 0)");
            }
        }

        if (additionalParams[N_PARAM_INDEX]) {
            try {
                limit = Long.parseLong(properties.getProperty(N_PARAM));
                if (limit <= 0) throw new NumberFormatException();
            } catch (NumberFormatException e) {
                throw new ArgumentException("n number must be supplied using -Dn and it must be a positive number");
            }
        }

        if (additionalParams[NAME_PARAM_INDEX]) {
            treeName = Optional.ofNullable(properties.getProperty(NAME_PARAM)).orElseThrow(new ArgumentException("Tree name must be supplied using -Dname"));
        }
    }

    private static ClientConfig getClientConfig() {
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("g6-cluster").setPassword("123456");
        config.getNetworkConfig().addAddress(clusterAddresses.toArray(new String[0]));
        config.setProperty("hazelcast.logging.type", "slf4j");
        return config;
    }

    private static void runQuery(Job<String, Tree> job, JobTracker jobTracker, HazelcastInstance hz)
            throws InterruptedException, ExecutionException, IOException {

        final Map<String, Long> neighbourhoods;
        switch (query) {
            case "query1":
                logger.info("Inicio de la lectura del archivo barrios");
                neighbourhoods = Parser.parseNeighbourhood(inPath, city);
                logger.info("Inicio de la lectura del archivo barrios");
                ClientUtils.genericSetCSVPrinter(outPath + query + ".csv", Query1.runQuery(job, neighbourhoods), Query1.print, Query1.HEADER);
                break;
            case "query2":
                logger.info("Inicio de la lectura del archivo barrios");
                neighbourhoods = Parser.parseNeighbourhood(inPath, city);
                logger.info("Inicio de la lectura del archivo barrios");
                ClientUtils.genericMapCSVPrinter(outPath + query + ".csv", Query2.runQuery(job, neighbourhoods, minNumber), Query2.print, Query2.HEADER);
                break;
            case "query3":
                ClientUtils.genericSetCSVPrinter(outPath + query + ".csv", Query3.runQuery(job, limit), Query3.print, Query3.HEADER);
                break;
            case "query4":
                ClientUtils.genericSetCSVPrinter(outPath + query + ".csv", Query4.runQuery(job, treeName, minNumber), Query4.print, Query4.HEADER);
                break;
            case "query5":
                ClientUtils.genericSetCSVPrinter(outPath + query + ".csv", Query5.runQuery(job, jobTracker, hz), Query5.print, Query5.HEADER);
                break;
            default:
                break;
        }
    }

    private static boolean[] getAdditionalParams() {
        // min, name, n
        switch (query) {
            case "query2":
                return new boolean[]{true, false, false};
            case "query3":
                return new boolean[]{false, false, true};
            case "query4":
                return new boolean[]{true, true, false};
            default:
                return new boolean[]{false, false, false};
        }
    }
}