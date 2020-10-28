package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query3Collator;
import ar.edu.itba.pod.tpe.mappers.NameDiameterMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.SumAvgReducerFactory;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Class with static methods dedicated to solve the Query 3
 */
public class Query3 {

    /**
     * Created the MapReduce with the corresponding classes
     * @param job The job created from the IMap instance with neighbourhoods and list of trees
     * @param limit The maximum value of results to show
     * @return The sorted set returned with the resulting values of the MapReduce
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static NavigableSet<ComparablePair<Double, String>> runQuery(Job<String, Tree> job, long limit)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<NavigableSet<ComparablePair<Double, String>>> future = job
                .mapper(new NameDiameterMapper())
                .reducer(new SumAvgReducerFactory<>())
                .submit(new Query3Collator(limit));
        return future.get();
    }

    /**
     * CSV header for this specific query
     */
    public static final String HEADER = "NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO";

    /**
     * Throwable consumer, printing method used for each value of the set
     */
    public static final ThrowableBiConsumer<ComparablePair<Double, String>, CSVPrinter, IOException> print = (e, p) -> {
        DecimalFormat df = new DecimalFormat("#0.00", new DecimalFormatSymbols(Locale.ENGLISH));
        df.setRoundingMode(RoundingMode.DOWN);
        p.printRecord(e.getSecond(), df.format(e.getFirst()));
    };
}
