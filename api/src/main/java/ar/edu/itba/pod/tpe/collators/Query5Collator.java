package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Receives entries with String, Long
 * Removes all keys with less than minCount value
 * Returns Sorted set of Comparable Pairs with cross join of this keys (k1 < k2)
 */
public class Query5Collator implements Collator<Map.Entry<String, Long>, SortedSet<ComparableTrio<Long, String, String>>> {
    private static final int min = 999;

    @Override
    public SortedSet<ComparableTrio<Long, String, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {
        // neighbourhoods has a set of
        Set<String> neighbourhoods = new HashSet<>();
        for (Map.Entry<String, Long> element : iterable) {
            if (element.getValue() >= minCount)
                neighbourhoods.add(element.getKey());
        }

        SortedSet<ComparableTrio<Long, String, String>> out = new TreeSet<>(ComparableTrio::compareTo);
        for (String first : neighbourhoods) {
            for (String second : neighbourhoods) {
                if (first.compareTo(second) < 0) {
                    out.add(new ComparablePir<>(first, second));
                }
            }
        }
        return out;
    }
}
