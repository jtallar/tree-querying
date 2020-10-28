package ar.edu.itba.pod.tpe.client.queries;

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

/**
 * Class with static methods dedicated to solve the Query 2
 */
public class Query2 {

    /**
     * Created the MapReduce with the corresponding classes
     * @param job The job created from the IMap instance with neighbourhoods and list of trees
     * @param neighbourhoods Map with valid neighbourhoods and their respective population
     * @param min The minimum amount of trees per street
     * @return The Map returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static Map<String,ComparablePair<String,Long>> runQuery(Job<Neighbourhood, List<Tree>> job, Map<String, Long>  neighbourhoods, long min)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<Map<String,ComparablePair<String,Long>>> future = job
                .keyPredicate(new NeighbourhoodKeyPredicate(neighbourhoods))
                .mapper(new StreetTreeMapper())
                .reducer(new SumReducerFactory<>())
                .submit(new Query2Collator(min));
        return future.get();
    }

    /**
     * CSV header for this specific query
     */
    public static final String HEADER = "BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES";

    /**
     * Throwable consumer, printing method used for each entry of the map
     */
    public static final ThrowableBiConsumer<Map.Entry<String, ComparablePair<String, Long>>, CSVPrinter, IOException> print = (e, p) -> {
        p.printRecord(e.getKey(), e.getValue().getFirst(), e.getValue().getSecond());
    };
}
