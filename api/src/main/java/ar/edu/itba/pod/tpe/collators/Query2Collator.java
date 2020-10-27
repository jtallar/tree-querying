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

        for(Map.Entry<Street,Long> elem : iterable){
            if(elem.getValue() >= min){
                String aux = elem.getKey().getNeighbourhood();
                if(!out.containsKey(aux)){
                    out.put(aux,
                            new ComparablePair<>(elem.getKey().getStreet(),elem.getValue()));
                }
                else if(out.get(aux).getSecond() < elem.getValue()){
                    out.get(aux).setSecond(elem.getValue());
                    out.get(aux).setFirst(elem.getKey().getStreet());
                }
            }
        }

        return out;
    }
}
