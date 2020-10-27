package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

/**
 * Given a key-value pair, with the neighbourhood as key and a list of trees as value,
 * emits 1 for each tree (n this case, the size of the list), with its neighbourhood name as the key.
 */
public class NeighbourhoodTreeMapper implements Mapper<Neighbourhood, List<Tree>, String, Long> {
    private static final long serialVersionUID = 8608574752502124582L;

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<String, Long> context) {
        context.emit(neighbourhood.getName(), (long) trees.size());
    }
}
