package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Street;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Receives entries with Street, Long
 * Returns Sorted Map of Comparable Pairs of each neighbourhood street with the most trees
 */
public class Query2Collator implements Collator<Map.Entry<Street, Long>, Map<String, ComparablePair<String, Long>>> {
    private final long min;

    public Query2Collator(long min) {
        this.min = min;
    }

    @Override
    public Map<String, ComparablePair<String, Long>> collate(Iterable<Map.Entry<Street, Long>> iterable) {

        Map<String, ComparablePair<String,Long>> out = new TreeMap<>(String::compareTo);
        iterable.forEach(e -> {
            if (e.getValue() <= min) return;
            ComparablePair<String, Long> pair = new ComparablePair<>(e.getKey().getStreet(), e.getValue());
            out.merge(e.getKey().getNeighbourhood(), pair, (prev, curr) -> (curr.getSecond() > prev.getSecond())? curr : prev);
        });
        return out;
    }
}
