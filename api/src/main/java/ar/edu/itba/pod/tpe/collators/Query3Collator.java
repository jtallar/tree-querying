package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Receives entries with Sting, Double
 * Returns Sorted set of Top (limit) Comparable Pairs
 */
public class Query3Collator implements Collator<Map.Entry<String, Double>, NavigableSet<ComparablePair<Double, String>>> {
    private final long limit;

    public Query3Collator(long limit) {
        this.limit = limit;
    }

    @Override
    public NavigableSet<ComparablePair<Double, String>> collate(Iterable<Map.Entry<String, Double>> iterable) {

        NavigableSet<ComparablePair<Double, String>> out = new TreeSet<>(ComparablePair::compareToModified);
        iterable.forEach(e -> out.add(new ComparablePair<>(e.getValue(), e.getKey())));
        return out.stream().limit(limit).collect(Collectors.toCollection(() -> new TreeSet<>(ComparablePair::compareToModified)));
    }
}
