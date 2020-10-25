package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query4Collator;
import ar.edu.itba.pod.tpe.mappers.Query4Mapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query4ReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

// TODO: VER SI SE PUEDE HACER UNA INTERFAZ PARA ESTO
public class Query4 {
    private static final String QUERY_HEADER = "Barrio A;Barrio B";

    public static void runQuery(Job<Neighbourhood, List<Tree>> job, String treeName, int minNumber, String outPath)
            throws InterruptedException, ExecutionException {
        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new Query4Mapper(treeName))
                .reducer(new Query4ReducerFactory())
                .submit(new Query4Collator(minNumber));

        // Wait and retrieve result
        SortedSet<ComparablePair<String, String>> result;
        result = future.get();

        ClientUtils.genericCSVPrinter(outPath + "query4.csv", result, printQuery);
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
