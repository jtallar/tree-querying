package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

/**
 * Receives a tree name as a parameter in constructor. To be used as a condition.
 * Given a key-value pair, with the neighbourhood as key and a list of trees as value,
 * emits a 1 (one) for each tree meeting the condition, with its neighbourhood as a key.
 * The processing is made in parallel to make it faster, as the order has no relevance.
 */
public class NeighbourhoodTreeFilteredMapper implements Mapper<Neighbourhood, List<Tree>, String, Long> {
    private static final long serialVersionUID = 1909833743234320270L;
    private static final Long ONE = 1L;
    private final String treeName;

    public NeighbourhoodTreeFilteredMapper(String treeName) {
        this.treeName = treeName;
    }

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<String, Long> context) {
        trees.parallelStream().filter(t -> treeName.equals(t.getCommonName()))
                .parallel().forEach(t -> context.emit(neighbourhood.getName(), ONE));
    }
}
