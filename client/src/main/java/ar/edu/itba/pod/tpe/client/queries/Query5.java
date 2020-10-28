package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query5Collator;
import ar.edu.itba.pod.tpe.mappers.InverterMapper;
import ar.edu.itba.pod.tpe.mappers.NeighbourhoodTreeMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.NavigableSetReducerFactory;
import ar.edu.itba.pod.tpe.reducers.SumReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.*;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

/**
 * Class with static methods dedicated to solve the Query 5
 */
public class Query5 {

    /**
     * Concatenated the two MapReduce stages
     * @param job The job created from the IMap instance with neighbourhoods and list of trees
     * @param jobTracker job tracker to add the second stage of the query
     * @param hz The HazelCast Instance for the same reason as the Job Tracker
     * @return The sorted set returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static SortedSet<ComparableTrio<Long, String, String>> runQuery(Job<Neighbourhood, List<Tree>> job, JobTracker jobTracker, HazelcastInstance hz)
            throws InterruptedException, ExecutionException {

        final IMap<String, Long> map = hz.getMap("g6-map-query5-aux");
        map.clear();
        map.putAll(runFirstMapReduce(job));

        final KeyValueSource<String, Long> source = KeyValueSource.fromMap(map);
        final Job<String, Long> secondJob = jobTracker.newJob(source);
        return runSecondMapReduce(secondJob);
    }

    /**
     * CSV header for this specific query
     */
    public static final String HEADER = "Grupo;Barrio A;Barrio B";

    /**
     * Throwable consumer, printing method used for each value of the set
     */
    public static final ThrowableBiConsumer<ComparableTrio<Long, String, String>, CSVPrinter, IOException> print = (e, p) -> {
        p.printRecord(e.getFirst(), e.getSecond(), e.getThird());
    };

    /**
     * Created the first MapReduce with the corresponding classes
     * @param job The job created from the IMap instance with neighbourhoods and list of trees
     * @return The map returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static Map<String, Long> runFirstMapReduce(Job<Neighbourhood, List<Tree>> job)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<Map<String, Long>> future = job
                .mapper(new NeighbourhoodTreeMapper())
                .reducer(new SumReducerFactory<>(3))
                .submit();
        return future.get();
    }

    /**
     * Created the MapReduce with the corresponding classes
     * @param job The job created from the IMap instance with neighbourhoods and its amount of trees
     * @return The sorted set returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static NavigableSet<ComparableTrio<Long, String, String>> runSecondMapReduce(Job<String, Long> job)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<NavigableSet<ComparableTrio<Long, String, String>>> future = job
                .mapper(new InverterMapper())
                .reducer(new NavigableSetReducerFactory<>())
                .submit(new Query5Collator());
        return future.get();
    }
}
