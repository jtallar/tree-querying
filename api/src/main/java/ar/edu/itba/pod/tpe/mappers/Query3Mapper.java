package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;


/**
 * Recieves treeName as a parameter in constructor
 * Emits 1 for each tree
 */
public class Query3Mapper implements Mapper<Neighbourhood, List<Tree>, String, Double> {
    private static final long serialVersionUID = 3909833743234320270L;

    public Query3Mapper() {}

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<String, Double> context) {
        for (Tree tree : trees) {
            context.emit(tree.getCommon_name(), tree.getDiameter());
        }
    }
}
