package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query1Collator;
import ar.edu.itba.pod.tpe.keyPredicates.Query1KeyPredicate;
import ar.edu.itba.pod.tpe.mappers.Query1Mapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query1ReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query5 {
    private static final String QUERY_HEADER = "Grupo;Barrio A;Barrio B";

    public static void runQuery(Job<Neighbourhood, List<Tree>> job, String outPath)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<SortedSet<ComparableTrio<String, String, String>>> future = job
                .mapper(new Query1Mapper())
                .reducer(new Query1ReducerFactory()) // same as query 4
                .submit(new Query1Collator());

        // Wait and retrieve result
        SortedSet<ComparableTrio<String,String, String>> result;
        result = future.get();

        ClientUtils.genericCSVPrinter(outPath + "query1.csv", result, printQuery);

    }

    /**
     * Throwable consumer, prints the result as a BarrioA;BarrioB
     */
    private static final ThrowableBiConsumer<SortedSet<ComparableTrio<String, String, String>>, CSVPrinter, IOException> printQuery = (results, printer) -> {
        // Print header and fill csv with results
        printer.printRecord(QUERY_HEADER);
        results.forEach(p -> {
            try {
                printer.printRecord(p.getFirst(), p.getSecond(), p.getThird());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    };
}
