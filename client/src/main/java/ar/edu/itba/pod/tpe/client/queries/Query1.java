package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
import ar.edu.itba.pod.tpe.client.utils.ThrowableBiConsumer;
import ar.edu.itba.pod.tpe.collators.Query1Collator;
import ar.edu.itba.pod.tpe.keyPredicates.NeighbourhoodKeyPredicate;
import ar.edu.itba.pod.tpe.mappers.NeighbourhoodTreeMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
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


public class Query1 {

    public static Set<ComparablePair<Double, String>> runQuery(Job<Neighbourhood, List<Tree>> job, Map<String, Long> neighbourhoods)
            throws InterruptedException, ExecutionException  {

        final JobCompletableFuture<Set<ComparablePair<Double, String>>> future = job
                .keyPredicate(new NeighbourhoodKeyPredicate(neighbourhoods))
                .mapper(new NeighbourhoodTreeMapper())
                .reducer(new SumReducerFactory<>())
                .submit(new Query1Collator(neighbourhoods));
        return future.get();
    }

    /**
     * CSV header for this specific query
     */
    public static final String HEADER = "GRUPO;ARBOLES_POR_HABITANTE";

    /**
     * Throwable consumer, prints the result as needed by this query
     */
    public static final ThrowableBiConsumer<ComparablePair<Double, String>, CSVPrinter, IOException> print = (e, p) -> {
        DecimalFormat df = new DecimalFormat("#0.00", new DecimalFormatSymbols(Locale.ENGLISH));
        p.printRecord(e.getSecond(), df.format(e.getFirst()));
    };

}
