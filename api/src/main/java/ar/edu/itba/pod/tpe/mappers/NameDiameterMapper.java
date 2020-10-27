package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

/**
 * Given a key-value pair, with the neighbourhood as key and a list of trees as value,
 * emits the diameter for each tree, with its common name as the key.
 * The processing is made in parallel to make it faster, as the order has no relevance.
 */
public class NameDiameterMapper implements Mapper<Neighbourhood, List<Tree>, String, Double> {
    private static final long serialVersionUID = 3909833743234320270L;

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<String, Double> context) {
        trees.parallelStream().forEach(t -> context.emit(t.getCommonName(), t.getDiameter()));
    }
}
