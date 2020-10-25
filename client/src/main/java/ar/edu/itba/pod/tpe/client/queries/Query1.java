package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query1Collator;
import ar.edu.itba.pod.tpe.collators.Query4Collator;
import ar.edu.itba.pod.tpe.keyPredicates.Query1KeyPredicate;
import ar.edu.itba.pod.tpe.mappers.Query1Mapper;
import ar.edu.itba.pod.tpe.mappers.Query4Mapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query1ReducerFactory;
import ar.edu.itba.pod.tpe.reducers.Query4ReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;


public class Query1 {
    private static final String QUERY_HEADER = "BARRIO;ARBOLES_POR_HABITANTE";

    public static void runQuery(Job<Neighbourhood, List<Tree>> job, Map<String, Integer> neighbours, String outPath)
            throws InterruptedException, ExecutionException  {
        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .keyPredicate(new Query1KeyPredicate(neighbours))
                .mapper(new Query1Mapper())
                .reducer(new Query1ReducerFactory()) // same as query 4
                .submit(new Query1Collator());

        // Wait and retrieve result
        SortedSet<ComparablePair<String, String>> result;
        result = future.get();

        ClientUtils.genericCSVPrinter(outPath + "query1.csv", result, printQuery);

    }

    /**
     * Throwable consumer, prints the result as a BarrioA;BarrioB
     */
    private static final ThrowableBiConsumer<SortedSet<ComparablePair<String, String>>, CSVPrinter, IOException> printQuery = (results, printer) -> {
        // Print header and fill csv with results
        printer.printRecord(QUERY_HEADER);
        results.forEach(p -> {
            try {
                printer.printRecord(p.getFirst(), p.getSecond());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    };

}
