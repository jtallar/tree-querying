package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Query1Collator implements Collator<Map.Entry<String, Long>, SortedSet<ComparablePair<String, String>>> {

    private final Map<String, Long> neighborhoods;

    public Query1Collator(Map<String, Long> neighborhoods) {
        this.neighborhoods = neighborhoods;
    }

    @Override
    public SortedSet<ComparablePair<String, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {

        SortedSet<ComparablePair<String, String>> out = new TreeSet<>(ComparablePair::compareTo);
        iterable.forEach(e -> out.add(new ComparablePair<>(e.getKey(), String.valueOf((float) e.getValue() / neighborhoods.get(e.getKey())))));
        return out;
    }
}
