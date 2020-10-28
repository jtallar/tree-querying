package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Receives entries with String, Long
 * Returns Sorted set of Comparable Pairs with the neighbourhoods name and its tree per population ratio
 */
public class Query1Collator implements Collator<Map.Entry<String, Long>, NavigableSet<ComparablePair<Double, String>>> {
    private final Map<String, Long> neighborhoods;

    public Query1Collator(Map<String, Long> neighborhoods) {
        this.neighborhoods = neighborhoods;
    }

    @Override
    public NavigableSet<ComparablePair<Double, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {

        NavigableSet<ComparablePair<Double, String>> out = new TreeSet<>(ComparablePair::compareToModified);
        iterable.forEach(e -> out.add(new ComparablePair<>((double) e.getValue() / neighborhoods.get(e.getKey()), e.getKey())));
        return out;
    }
}
