package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.models.TreeStreet;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TreeStreetMapper implements Mapper<String, Tree, TreeStreet,Long> {
    private static final Long ONE = 1L;

    @Override
    public void map(String s, Tree tree, Context<TreeStreet, Long> context) {
        context.emit(new TreeStreet(tree.getNeighbourhood_name(),tree.getCommon_name()),ONE);
    }
}
