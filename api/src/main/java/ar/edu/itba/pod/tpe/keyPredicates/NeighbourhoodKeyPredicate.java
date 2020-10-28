package ar.edu.itba.pod.tpe.keyPredicates;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;

/**
 * Receives on constructor a map with valid neighbourhoods,
 * and checks from the key-value pair, if the key is a known
 * neighbourhood. If its not, then omit processing.
 */
public class NeighbourhoodKeyPredicate implements KeyPredicate<Neighbourhood> {
    private static final long serialVersionUID = 7342802207091432862L;
    private Map<String, Long> neighbourhoodMap;

    public NeighbourhoodKeyPredicate(Map<String, Long> neighbourhoodMap) {
        this.neighbourhoodMap = neighbourhoodMap;
    }

    @Override
    public boolean evaluate(Neighbourhood neighbourhood) {
        return neighbourhoodMap.containsKey(neighbourhood.getName());
    }
}
