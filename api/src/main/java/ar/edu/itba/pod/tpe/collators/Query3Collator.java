package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Receives entries with String, Long
 * Returns Sorted set of Comparable Pairs with cross join of this keys (k1 < k2)
 */
public class Query3Collator implements Collator<Map.Entry<String, Double>, SortedSet<ComparablePair<String, Double>>> {

    private final long limit;

    public Query3Collator(long limit) {
        this.limit = limit;
    }

    @Override
    public SortedSet<ComparablePair<String, Double>> collate(Iterable<Map.Entry<String, Double>> iterable) {

        Comparator<ComparablePair<String, Double>> comparator = Comparator.comparing(ComparablePair::getSecond);
        comparator = comparator.reversed();
        comparator = comparator.thenComparing(ComparablePair::getFirst);
        final Comparator<ComparablePair<String, Double>> comparatorFinal = comparator;

        SortedSet<ComparablePair<String, Double>> out = new TreeSet<>(comparator);

        for(Map.Entry<String, Double> element : iterable){
            out.add(new ComparablePair<>(element.getKey(), element.getValue()));
        }
        Supplier<TreeSet<ComparablePair<String, Double>>> supplier = () -> new TreeSet<>(comparatorFinal);
        out = out.stream().limit(limit).collect(Collectors.toCollection(supplier));

        return out;
    }
}
