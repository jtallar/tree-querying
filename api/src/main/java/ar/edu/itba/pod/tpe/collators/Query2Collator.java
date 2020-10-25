package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.models.TreeStreet;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query2Collator implements Collator<Map.Entry<TreeStreet,Long>,Map<TreeStreet, Long>> {

    private final int min;

    public Query2Collator(int min) {
        this.min = min;
    }

    @Override
    public Map<TreeStreet, Long> collate(Iterable<Map.Entry<TreeStreet, Long>> iterable) {
        Map<TreeStreet, Long> t = new HashMap<>();
        for(Map.Entry<TreeStreet,Long> elem : iterable){
            if(elem.getValue() >= min){
                t.put(elem.getKey(),elem.getValue());
            }
        }
        return t;
    }
}
