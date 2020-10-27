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

    public static SortedSet<ComparablePair<String, String>> runQuery(Job<Neighbourhood, List<Tree>> job, String treeName, long minNumber)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new NeighbourhoodTreeFilteredMapper(treeName))
                .reducer(new SumReducerFactory<>())
                .submit(new Query4Collator(minNumber));
        return future.get();
    }

    public static final String HEADER = "Barrio A;Barrio B";

    public static final ThrowableBiConsumer<ComparablePair<String, String>, CSVPrinter, IOException> print = (e, p) -> {
        p.printRecord(e.getFirst(), e.getSecond());
    };
}
