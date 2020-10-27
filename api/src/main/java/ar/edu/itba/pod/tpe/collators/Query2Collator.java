package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.Street;
import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query2Collator implements
        Collator<Map.Entry<Street,Long>,Map<String, ComparablePair<String,Long>>> {

    private final long min;

    public Query2Collator(long min) {
        this.min = min;
    }

    @Override
    public Map<String, ComparablePair<String,Long>> collate(Iterable<Map.Entry<Street, Long>> iterable) {

        Map<String, ComparablePair<String,Long>> out = new TreeMap<>(String::compareTo);
        iterable.forEach(e -> {
            if (e.getValue() < min) return;
//            out.
            String aux = e.getKey().getNeighbourhood();
//            out.computeIfPresent(aux)
            if(!out.containsKey(aux)) { // si no esta pongo
                out.put(aux, new ComparablePair<>(e.getKey().getStreet(),e.getValue()));
            } else if(out.get(aux).getSecond() < e.getValue()) { // si esta, me fijo si cantidad es mayor, en tal caso actualizo
                out.get(aux).setSecond(e.getValue());
                out.get(aux).setFirst(e.getKey().getStreet());
            }

        });
        return out;
    }
}
