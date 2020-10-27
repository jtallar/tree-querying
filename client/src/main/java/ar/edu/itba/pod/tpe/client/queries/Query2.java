package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query2Collator;
import ar.edu.itba.pod.tpe.keyPredicates.NeighbourhoodKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.StreetTreeMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query2Reducer;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query2 {
    private static final String QUERY_HEADER = "BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES";

    public static void runQuery(Job<Neighbourhood, List<Tree>> job, Map<String, Long>  neigh, long minNumber, String outPath)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<Map<String,ComparablePair<String,Long>>> future = job
                .keyPredicate(new NeighbourhoodKeyPredicate(neigh))
                .mapper(new StreetTreeMapper())
                .reducer(new Query2Reducer())
                .submit(new Query2Collator(minNumber));

        // Wait and retrieve result
        Map<String,ComparablePair<String,Long>> result;
        result = future.get();

        ClientUtils.mapSVPrinter(outPath + "query2.csv", result, printQuery);
    }

    // TODO: Integrar ambas, que solo se usa esta que devuelve y que tenga otra llamada para printear
    public static Map<String,ComparablePair<String,Long>> runQueryTest(Job<Neighbourhood, List<Tree>> job, Map<String, Long>  neigh, long minNumber)
            throws InterruptedException, ExecutionException {
        final JobCompletableFuture<Map<String,ComparablePair<String,Long>>> future = job
                .keyPredicate(new NeighbourhoodKeyPredicate(neigh))
                .mapper(new StreetTreeMapper())
                .reducer(new Query2Reducer())
                .submit(new Query2Collator(minNumber));

        // Wait and retrieve result
        return future.get();
    }

    private static final ThrowableBiConsumer<Map<String,ComparablePair<String,Long>> , CSVPrinter, IOException> printQuery = (results, printer) -> {
        // Print header and fill csv with results
        printer.printRecord(QUERY_HEADER);
        results.forEach( (p, q) -> {
            try {
                printer.printRecord(p, q.getFirst(), q.getSecond());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    };
}
