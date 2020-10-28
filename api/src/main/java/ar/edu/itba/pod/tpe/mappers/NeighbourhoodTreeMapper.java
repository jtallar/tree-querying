package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import java.util.Map;

/**
 * Can recieve a map with neighbourhoods, population (value not used)
 * Given a key-value pair, with a tree as value,
 * emits 1 for each tree , with its neighbourhood name as the key.
 * If map given, checks if it contains the tree neighbourhood's name
 */
public class NeighbourhoodTreeMapper implements Mapper<String, Tree, String, Long> {
    private static final long serialVersionUID = 8608574752502124582L;
    private static final Long ONE = 1L;

    private final Map<String, Long> neighbourhoods;

    public NeighbourhoodTreeMapper() {
        neighbourhoods = null;
    }

    public NeighbourhoodTreeMapper(Map<String, Long> neighbourhoods) {
        this.neighbourhoods = neighbourhoods;
    }

    @Override
    public void map(String s, Tree tree, Context<String, Long> context) {
        if (neighbourhoods == null || neighbourhoods.containsKey(tree.getNeighbourhoodName())) {
            context.emit(tree.getNeighbourhoodName(), ONE);
        }
    }
}
