package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

/**
 * Receives nothing as a parameter in constructor
 * Emits 1 for each tree
 */
public class Query5Mapper implements Mapper<Neighbourhood, List<Tree>, String, Long> {
    private static final long serialVersionUID = 505973575697406692L;

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<String, Long> context) {
        context.emit(neighbourhood.getName(), (long) trees.size());
    }
}
