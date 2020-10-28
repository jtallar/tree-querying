package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Receives a tree name as a parameter in constructor. To be used as a condition.
 * Given a key-value pair, with a tree as value,
 * emits a 1 (one) for each tree meeting the condition, with its neighbourhood as a key.
 */
public class NeighbourhoodTreeFilteredMapper implements Mapper<String, Tree, String, Long> {
    private static final long serialVersionUID = 1909833743234320270L;
    private static final Long ONE = 1L;
    private final String treeName;

    public NeighbourhoodTreeFilteredMapper(String treeName) {
        this.treeName = treeName;
    }

    @Override
    public void map(String s, Tree tree, Context<String, Long> context) {
        if (treeName.equals(tree.getCommonName())) {
            context.emit(tree.getNeighbourhoodName(), ONE);
        }
    }
}
