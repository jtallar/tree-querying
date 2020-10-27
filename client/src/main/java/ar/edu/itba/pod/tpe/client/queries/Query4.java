package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query4Collator;
import ar.edu.itba.pod.tpe.mappers.NeighbourhoodTreeFilteredMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.SumReducerFactory;
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

    public static void runQuery(Job<Neighbourhood, List<Tree>> job, String treeName, long minNumber, String outPath)
            throws InterruptedException, ExecutionException {
        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new NeighbourhoodTreeFilteredMapper(treeName))
                .reducer(new SumReducerFactory<>())
                .submit(new Query4Collator(minNumber));

        // Wait and retrieve result
        SortedSet<ComparablePair<String, String>> result;
        result = future.get();

        ClientUtils.genericCSVPrinter(outPath + "query4.csv", result, printQuery);
    }

    // TODO: Integrar ambas, que solo se usa esta que devuelve y que tenga otra llamada para printear
    public static SortedSet<ComparablePair<String, String>> runQueryTest(Job<Neighbourhood, List<Tree>> job, String treeName, long minNumber)
            throws InterruptedException, ExecutionException {
        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new NeighbourhoodTreeFilteredMapper(treeName))
                .reducer(new SumReducerFactory<>())
                .submit(new Query4Collator(minNumber));

        // Wait and retrieve result
        return future.get();
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
