package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.client.utils.ClientUtils;
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
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query3 {

    public static SortedSet<ComparablePair<Double, String>> runQuery(Job<Neighbourhood, List<Tree>> job, long limit)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<SortedSet<ComparablePair<Double, String>>> future = job
                .mapper(new NameDiameterMapper())
                .reducer(new SumAvgReducerFactory<>())
                .submit(new Query3Collator(limit));
        return future.get();
    }

    public static final String HEADER = "NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO";

    public static final ThrowableBiConsumer<ComparablePair<Double, String>, CSVPrinter, IOException> print = (e, p) -> {
        DecimalFormat df = new DecimalFormat("#0.00", new DecimalFormatSymbols(Locale.ENGLISH));
        p.printRecord(e.getSecond(), df.format(e.getFirst()));
    };
}
