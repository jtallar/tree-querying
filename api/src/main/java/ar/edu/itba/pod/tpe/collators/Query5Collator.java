package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Receives entries with Long, String
 * Removes the key with 0.
 * Returns Sorted set of Comparable Trios
 */
public class Query5Collator implements Collator<Map.Entry<Long, NavigableSet<String>>, NavigableSet<ComparableTrio<Long, String, String>>> {

    @Override
    public NavigableSet<ComparableTrio<Long, String, String>> collate(Iterable<Map.Entry<Long, NavigableSet<String>>> iterable) {

        NavigableSet<ComparableTrio<Long, String, String>> out = new TreeSet<>(ComparableTrio::compareToModified);
        iterable.forEach(e -> addCombinations(out, e.getKey(), e.getValue().first(), e.getValue().tailSet(e.getValue().first(), false)));
        return out;
    }

    /**
     * Recursive function to create all pairs for a given set
     * @param out The set to save all the generated pairs
     * @param key Key number, shared across all pairs
     * @param first The first (ordered) neighbourhood of the set, make pairs with the rest
     * @param set The rest of the neighbourhoods with the same key
     */
    private static void addCombinations(SortedSet<ComparableTrio<Long, String, String>> out, Long key, String first, NavigableSet<String> set) {
        if (set.size() == 0) return;
        out.add(new ComparableTrio<>(key, first, set.first()));
        addCombinations(out, key, first, set.tailSet(set.first(), false));
        addCombinations(out, key, set.first(), set.tailSet(set.first(), false));
    }
}
