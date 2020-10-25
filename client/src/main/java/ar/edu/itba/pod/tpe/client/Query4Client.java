package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query4Collator;
import ar.edu.itba.pod.tpe.keyPredicates.TestKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.Query4Mapper;
import ar.edu.itba.pod.tpe.mappers.TestMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query4ReducerFactory;
import ar.edu.itba.pod.tpe.reducers.TestReducer;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class Query4Client {
    private static Logger logger = LoggerFactory.getLogger(Query4Client.class);

    private static final String CITY_PARAM = "city";
    private static final String ADDRESS_PARAM = "addresses";
    private static final String IN_PATH_PARAM = "inPath";
    private static final String OUT_PATH_PARAM = "outPath";

    private static final String MIN_PARAM = "min";
    private static final String NAME_PARAM = "name";
    private static final String N_PARAM = "n";

    private static final String QUERY4_HEADER = "Barrio A;Barrio B";
    private static final String QUERY4_FILE_NAME = "query4";


    private static final int ERROR_STATUS = 1;

    private static City city;
    private static List<String> clusterAddresses = new ArrayList<>();
    private static String inPath, outPath;
    private static int minNumber;
    private static String treeName;

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

        JobTracker jobTracker = hz.getJobTracker("g6-query4");

        final IMap<Neighbourhood, List<Tree>> map = hz.getMap("g6-query4");
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

        final KeyValueSource<Neighbourhood, List<Tree>> source = KeyValueSource.fromMap(map);
        final Job<Neighbourhood, List<Tree>> job = jobTracker.newJob(source);
        logger.info("Inicio del trabajo map/reduce");
        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new Query4Mapper(treeName))
                .reducer(new Query4ReducerFactory())
                .submit(new Query4Collator(minNumber));

        // Wait and retrieve result
        SortedSet<ComparablePair<String, String>> result;
        try {
             result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Future execution interrupted. Exiting...");
            System.exit(ERROR_STATUS);
            return;
        }

        genericCSVPrinter(outPath + QUERY4_FILE_NAME + ".csv", result, printQuery4);

        logger.info("Fin del trabajo map/reduce");

        hz.shutdown();
    }

    private static void argumentParsing() throws ArgumentException {
        Properties properties = System.getProperties();

        System.out.println(properties.getProperty(CITY_PARAM));
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

    /**
     * Opens the csv printer, executes the printing function, and closes the printer.
     * @param file File path for the csv printer.
     * @param result Result from the service response.
     * @param printFunction Print function to be applied.
     */
    private static void genericCSVPrinter(String file, SortedSet<ComparablePair<String, String>> result, ThrowableBiConsumer<SortedSet<ComparablePair<String, String>>, CSVPrinter, IOException> printFunction) {
        try (final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';')
                .withRecordSeparator('\n'))) {

            // Applies function to result and writes on the csv
            printFunction.accept(result, csvPrinter);

            // Closes the csv printer
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }

    /**
     * Throwable consumer, prints the result as a BarrioA;BarrioB
     */
    private static final ThrowableBiConsumer<SortedSet<ComparablePair<String, String>>, CSVPrinter, IOException> printQuery4 = (results, printer) -> {
        // Print header and fill csv with results
        printer.printRecord(QUERY4_HEADER);
        results.forEach(p -> {
            try {
                printer.printRecord(p.getFirst(), p.getSecond());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    };
}