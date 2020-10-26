package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Receives entries with String, Long
 * Returns Sorted set of Comparable Pairs with cross join of this keys (k1 < k2)
 */
public class Query3Collator implements Collator<Map.Entry<String, Double>, SortedSet<ComparablePair<String, Double>>> {

    private final int limit;

    public Query3Collator(int limit) {
        this.limit = limit;
    }

    @Override
    public SortedSet<ComparablePair<String, Double>> collate(Iterable<Map.Entry<String, Double>> iterable) {

        Comparator<ComparablePair<String, Double>> comparator = Comparator.comparing(ComparablePair::getSecond);
        comparator = comparator.thenComparing(ComparablePair::getFirst);

        return new TreeSet<>(comparator).stream().limit(limit).collect(Collectors.toCollection(TreeSet::new));
    }
}
