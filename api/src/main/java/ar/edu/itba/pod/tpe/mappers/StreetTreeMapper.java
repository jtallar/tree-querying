package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.models.Street;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import java.util.Map;

/**
 * Recieves a neighbourhood map wit Name, population (value not used)
 * Given a key-value pair, with a tree as value,
 * emits a 1 (one) for each tree, with its street as a key if neighbourhood exists in map.
 */
public class StreetTreeMapper implements Mapper<String, Tree, Street,Long> {
    private static final long serialVersionUID = 5186107447712636552L;
    private static final Long ONE = 1L;

    private final Map<String, Long> neighbourhoods;

    public StreetTreeMapper(Map<String, Long> neighbourhoods) {
        this.neighbourhoods = neighbourhoods;
    }

    @Override
    public void map(String s, Tree tree, Context<Street, Long> context) {
        if (neighbourhoods.containsKey(tree.getNeighbourhoodName())) {
            context.emit(new Street(tree.getNeighbourhoodName(), tree.getStreetName()), ONE);
        }
    }
}
