package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query2Collator;
import ar.edu.itba.pod.tpe.keyPredicates.NeighbourhoodKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.StreetTreeMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.SumReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query2 {

    public static Map<String,ComparablePair<String,Long>> runQuery(Job<Neighbourhood, List<Tree>> job, Map<String, Long>  neigh, long minNumber)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<Map<String,ComparablePair<String,Long>>> future = job
                .keyPredicate(new NeighbourhoodKeyPredicate(neigh))
                .mapper(new StreetTreeMapper())
                .reducer(new SumReducerFactory<>())
                .submit(new Query2Collator(minNumber));
        return future.get();
    }

    public static final String HEADER = "BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES";

    public static final ThrowableBiConsumer<Map.Entry<String, ComparablePair<String, Long>>, CSVPrinter, IOException> print = (e, p) -> {
        p.printRecord(e.getKey(), e.getValue().getFirst(), e.getValue().getSecond());
    };
}
