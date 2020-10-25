package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Query1Collator implements Collator<Map.Entry<Neighbourhood, Long>, SortedSet<ComparablePair<String, String>>> {

    @Override
    public SortedSet<ComparablePair<String, String>> collate(Iterable<Map.Entry<Neighbourhood, Long>> iterable) {

        SortedSet<ComparablePair<String, String>> out = new TreeSet<>(ComparablePair::compareTo);
        iterable.forEach(e -> {
            e.setValue(e.getValue()/e.getKey().getPopulation());
            out.add(new ComparablePair<>(e.getKey().getName(), e.getValue().toString()));
        });

        return out;
    }
}
