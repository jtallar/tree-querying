package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class Query1Collator implements Collator<Map.Entry<String, Long>, Set<ComparablePair<Double, String>>> {
    private static final int PLACES = 2;
    private final Map<String, Long> neighborhoods;

    public Query1Collator(Map<String, Long> neighborhoods) {
        this.neighborhoods = neighborhoods;
    }

    @Override
    public Set<ComparablePair<Double, String>> collate(Iterable<Map.Entry<String, Long>> iterable) {

        Set<ComparablePair<Double, String>> out = new TreeSet<>(ComparablePair::compareToModified);
        iterable.forEach(e -> out.add(new ComparablePair<>(round((double) e.getValue() / neighborhoods.get(e.getKey())), e.getKey())));
        return out;
    }

    private static double round(double value) {
        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(PLACES, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
