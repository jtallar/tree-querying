package ar.edu.itba.pod.tpe.keyPredicates;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;

public class TestKeyPredicate implements KeyPredicate<Neighbourhood> {
    private Map<String, Integer> neighbourhoodPopulation;

    public TestKeyPredicate(Map<String, Integer> neighbourhoodPopulation) {
        this.neighbourhoodPopulation = neighbourhoodPopulation;
    }

    @Override
    public boolean evaluate(Neighbourhood neighbourhood) {
        neighbourhood.setPopulation(neighbourhoodPopulation.getOrDefault(neighbourhood.getName(), 0));
        return true;
    }
}
