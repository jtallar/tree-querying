package ar.edu.itba.pod.tpe.client.queries;

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

/**
 * Class with static methods dedicated to solve the Query 4
 */
public class Query4 {

    /**
     * Created the MapReduce with the corresponding classes
     * @param job The job created from the IMap instance with neighbourhoods and list of trees
     * @param treeName Type of name to filter by
     * @param min The minimum quantity of trees needed per neighbourhood
     * @return The sorted set returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static SortedSet<ComparablePair<String, String>> runQuery(Job<Neighbourhood, List<Tree>> job, String treeName, long min)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<SortedSet<ComparablePair<String, String>>> future = job
                .mapper(new NeighbourhoodTreeFilteredMapper(treeName))
                .reducer(new SumReducerFactory<>())
                .submit(new Query4Collator(min));
        return future.get();
    }

    /**
     * CSV header for this specific query
     */
    public static final String HEADER = "Barrio A;Barrio B";

    /**
     * Throwable consumer, printing method used for each value of the set
     */
    public static final ThrowableBiConsumer<ComparablePair<String, String>, CSVPrinter, IOException> print = (e, p) -> {
        p.printRecord(e.getFirst(), e.getSecond());
    };
}
