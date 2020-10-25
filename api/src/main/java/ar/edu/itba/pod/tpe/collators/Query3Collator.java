package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Receives entries with String, Long
 * Returns Sorted set of Comparable Pairs with cross join of this keys (k1 < k2)
 */
public class Query3Collator implements Collator<Map.Entry<String, Double>, SortedSet<ComparablePair<String, String>>> {

    private int limit;

    public Query3Collator(int limit) {
        this.limit = limit;
    }

    @Override
    public SortedSet<ComparablePair<String, String>> collate(Iterable<Map.Entry<String, Double>> iterable) {

        // neighbourhoods has a set of
        Set<String> trees = new HashSet<>();
//        for (Map.Entry<String, Long> element : iterable) {
//            if (element.getValue() >= minCount)
//                neighbourhoods.add(element.getKey());
//        }

        SortedSet<ComparablePair<String, String>> out = new TreeSet<>(ComparablePair::compareTo);
        for (String first : trees) {
            for (String second : trees) {
                if (first.compareTo(second) < 0) {
                    out.add(new ComparablePair<>(first, second));
                }
            }
        }
        return out;
    }
}
