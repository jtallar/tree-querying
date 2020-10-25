package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Query1Collator implements Collator<Map.Entry<String, Long>, SortedSet<ComparablePair<Double, String>>> {

    private final Map<String, Long> neighborhoods;

    public Query1Collator(Map<String, Long> neighborhoods) {
        this.neighborhoods = neighborhoods;
    }

    @Override
    public SortedSet<ComparablePair<Double, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {

        SortedSet<ComparablePair<Double, String>> out = new TreeSet<>(ComparablePair::compareTo);
        iterable.forEach(e -> out.add(new ComparablePair<>((double) (e.getValue() / neighborhoods.get(e.getKey())), e.getKey())));
        return out;
    }
}
