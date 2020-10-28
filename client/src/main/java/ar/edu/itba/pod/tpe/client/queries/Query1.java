package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query1Collator;
import ar.edu.itba.pod.tpe.mappers.NeighbourhoodTreeMapper;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.SumReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Class with static methods dedicated to solve the Query 1
 */
// TODO: deberia listar los barrios con 0 arboles por habitante? Es decir, no tienen arboles en arboles.csv,
//    pero aparecen en barrios.csv

//  TODO: Estamos redondeando, no truncando
public class Query1 {

    /**
     * Created the MapReduce with the corresponding classes
     * @param job The job created from the IMap instance with neighbourhoods and list of trees
     * @param neighbourhoods Map with valid neighbourhoods and their respective population
     * @return The sorted set returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static NavigableSet<ComparablePair<Double, String>> runQuery(Job<String, Tree> job, Map<String, Long> neighbourhoods)
            throws InterruptedException, ExecutionException  {

        final JobCompletableFuture<NavigableSet<ComparablePair<Double, String>>> future = job
//                .keyPredicate(new NeighbourhoodKeyPredicate(neighbourhoods))
                .mapper(new NeighbourhoodTreeMapper(neighbourhoods))
                .reducer(new SumReducerFactory<>())
                .submit(new Query1Collator(neighbourhoods));
        return future.get();
    }

    /**
     * CSV header for this specific query
     */
    public static final String HEADER = "BARRIO;ARBOLES_POR_HABITANTE";

    /**
     * Throwable consumer, printing method used for each value of the set
     */
    public static final ThrowableBiConsumer<ComparablePair<Double, String>, CSVPrinter, IOException> print = (e, p) -> {
        DecimalFormat df = new DecimalFormat("#0.00", new DecimalFormatSymbols(Locale.ENGLISH));
        p.printRecord(e.getSecond(), df.format(e.getFirst()));
    };
}
