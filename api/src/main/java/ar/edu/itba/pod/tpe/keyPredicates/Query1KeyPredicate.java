package ar.edu.itba.pod.tpe.keyPredicates;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;
import java.util.Set;

public class Query1KeyPredicate implements KeyPredicate<Neighbourhood> {
    private final Map<String, Long> neighbourhoods;

    public Query1KeyPredicate(Map<String, Long> neighbourhoods) {
        this.neighbourhoods = neighbourhoods;
    }

    @Override
    public boolean evaluate(Neighbourhood neighbourhood) {
        return neighbourhoods.containsKey(neighbourhood.getName());
    }
}
