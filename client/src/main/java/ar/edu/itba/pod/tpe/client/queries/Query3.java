package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query3Collator;
import ar.edu.itba.pod.tpe.mappers.Query3Mapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query3ReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query3 {
    private static final String QUERY_HEADER = "NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO";

    public static void runQuery(Job<Neighbourhood, List<Tree>> job, int limit, String outPath)
            throws InterruptedException, ExecutionException {
        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new Query3Mapper())
                .reducer(new Query3ReducerFactory())
                .submit(new Query3Collator(limit));

        // Wait and retrieve result
        SortedSet<ComparablePair<String, String>> result;
        result = future.get();

        ClientUtils.genericCSVPrinter(outPath + "query3.csv", result, printQuery);
    }

    /**
     * Throwable consumer, prints the result as a NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO
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
