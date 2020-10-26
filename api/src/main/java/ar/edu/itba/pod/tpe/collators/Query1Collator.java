package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query1Collator implements Collator<Map.Entry<String, Long>, Set<ComparablePair<Double, String>>> {

    private final Map<String, Long> neighborhoods;

    public Query1Collator(Map<String, Long> neighborhoods) {
        this.neighborhoods = neighborhoods;
    }

    @Override
    public Set<ComparablePair<Double, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {

        Set<ComparablePair<Double, String>> out = new TreeSet<>(ComparablePair::compareToModified);
        iterable.forEach(e -> out.add(new ComparablePair<>((double) e.getValue() / neighborhoods.get(e.getKey()), e.getKey())));
        return out;
    }
}
