package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Receives entries with String, Long
 * Removes all keys with less than minCount value
 * Returns Sorted set of Comparable Pairs with cross join of this keys (k1 < k2)
 */
public class Query4Collator implements Collator<Map.Entry<String, Long>, SortedSet<ComparablePair<String, String>>> {
    private long minCount;

    public Query4Collator(long minCount) {
        this.minCount = minCount;
    }

    @Override
    public SortedSet<ComparablePair<String, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {
        // neighbourhoods has a set of
        Set<String> neighbourhoods = new HashSet<>();
        for (Map.Entry<String, Long> element : iterable) {
            if (element.getValue() >= minCount)
                neighbourhoods.add(element.getKey());
        }

        SortedSet<ComparablePair<String, String>> out = new TreeSet<>(ComparablePair::compareTo);
        for (String first : neighbourhoods) {
            for (String second : neighbourhoods) {
                if (first.compareTo(second) < 0) {
                    out.add(new ComparablePair<>(first, second));
                }
            }
        }
        return out;
    }
}
