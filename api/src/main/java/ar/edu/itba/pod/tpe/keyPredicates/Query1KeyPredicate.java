package ar.edu.itba.pod.tpe.keyPredicates;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;
import java.util.Set;

public class Query1KeyPredicate implements KeyPredicate<Neighbourhood> {
    private Set<String> neighbourhoods;

    public Query1KeyPredicate(Set<String> neighbourhoods) {
        this.neighbourhoods = neighbourhoods;
    }

    @Override
    public boolean evaluate(Neighbourhood neighbourhood) {
        return neighbourhoods.contains(neighbourhood.getName());
    }
}
