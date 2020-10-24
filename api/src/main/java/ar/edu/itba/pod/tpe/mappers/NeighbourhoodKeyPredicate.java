package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.KeyPredicate;

import java.io.Serializable;
import java.util.List;

public class NeighbourhoodKeyPredicate implements KeyPredicate<String>, Serializable {

    private List<Neighbourhood> neighbourhoodList;

    public NeighbourhoodKeyPredicate() {
    }

    public NeighbourhoodKeyPredicate(List<Neighbourhood> neighbourhoodList) {
        this.neighbourhoodList = neighbourhoodList;
    }

    @Override
    public boolean evaluate(String tree) {
        for(Neighbourhood n : neighbourhoodList){
            if(n.getName().equals(tree))
                return true;
        }
        return false;
    }
}
