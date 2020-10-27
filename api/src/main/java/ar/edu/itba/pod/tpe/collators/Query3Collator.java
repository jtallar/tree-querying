package ar.edu.itba.pod.tpe.collators;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

public class Query3Collator implements Collator<Map.Entry<String, Double>, SortedSet<ComparablePair<Double, String>>> {

    private final long limit;

    public Query3Collator(long limit) {
        this.limit = limit;
    }

    @Override
    public SortedSet<ComparablePair<Double, String>> collate(Iterable<Map.Entry<String, Double>> iterable) {


        NavigableSet<ComparablePair<Double, String>> out = new TreeSet<>(ComparablePair::compareToModified);
        iterable.forEach(e -> {
            if (out.size() < limit)
                out.add(new ComparablePair<>(e.getValue(), e.getKey()));
            else if (out.last().getFirst() < e.getValue()) {
                out.pollLast();
                out.add(new ComparablePair<>(e.getValue(), e.getKey()));
            }
        });
        return out;
    }
}

//iterable.forEach(e -> out.add(new ComparablePair<>(e.getValue(), e.getKey())));
//return out.stream().limit(limit).collect(Collectors.toCollection(() -> new TreeSet<>(ComparablePair::compareToModified)))
