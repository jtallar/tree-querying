package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Receives entries with Long, String
 * Removes the key with 0.
 * Returns Sorted set of Comparable Trios
 */
public class Query5BCollator implements Collator<Map.Entry<Long, NavigableSet<String>>, SortedSet<ComparableTrio<Long, String, String>>> {

    @Override
    public SortedSet<ComparableTrio<Long, String, String>> collate(Iterable<Map.Entry<Long, NavigableSet<String>>> iterable) {

        SortedSet<ComparableTrio<Long, String, String>> out = new TreeSet<>(ComparableTrio::compareTo);
        iterable.forEach(e -> System.out.println("Key=" + e.getKey() + ", Value=" + e.getValue() + "\n"));
//        iterable.forEach(e -> {
//            if (e.getKey() == 0) return;
//
//            while (!e.getValue().isEmpty()) {
//                String first = e.getValue().pollFirst();
//                e.getValue().iterator().forEachRemaining(s -> out.add(new ComparableTrio<>(e.getKey(), first, s)));
//            }
//        });
        return out;
    }


}
