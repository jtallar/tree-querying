package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;


/**
 * Recieves treeName as a parameter in constructor
 * Emits 1 for each tree that has the same name as treeName
 */
public class Query4Mapper implements Mapper<Neighbourhood, List<Tree>, String, Long> {
    private static final long serialVersionUID = 1909833743234320270L;

    private static final Long ONE = 1L;
    private final String treeName;

    public Query4Mapper(String treeName) {
        this.treeName = treeName;
    }

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<String, Long> context) {
        for (Tree tree : trees) {
            // TODO: ver que pasa si el min es 0, si debo seguir emitiendo unicamente para los que tienen este arbol
            if (treeName.equals(tree.getCommon_name())) {
                context.emit(neighbourhood.getName(), ONE);
            }
        }
    }
}
