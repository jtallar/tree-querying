package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.models.Street;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

/**
 * Given a key-value pair, with the neighbourhood as key and a list of trees as value,
 * emits a 1 (one) for each tree, with its street as a key.
 * The processing is made in parallel to make it faster, as the order has no relevance.
 */
public class StreetTreeMapper implements Mapper<Neighbourhood, List<Tree>, Street,Long> {
    private static final long serialVersionUID = 5186107447712636552L;
    private static final Long ONE = 1L;

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<Street, Long> context) {
        trees.parallelStream().forEach(t -> context.emit(new Street(neighbourhood.getName(), t.getStreetName()), ONE));
    }
}
