package ar.edu.itba.pod.tpe.keyPredicates;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.KeyPredicate;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class NeighbourhoodKeyPredicate implements KeyPredicate<Neighbourhood> {

    private Map<String, Integer> neighbourhoodMap;

    public NeighbourhoodKeyPredicate() {
    }

    public NeighbourhoodKeyPredicate(Map<String, Integer> neighbourhoodMap) {
        this.neighbourhoodMap = neighbourhoodMap;
    }

    @Override
    public boolean evaluate(Neighbourhood neighbourhood) {
        return neighbourhoodMap.containsKey(neighbourhood.getName());
    }

}
