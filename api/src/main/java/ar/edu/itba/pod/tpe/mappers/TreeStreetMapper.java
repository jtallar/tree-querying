package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.models.TreeStreet;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

public class TreeStreetMapper implements Mapper<Neighbourhood, List<Tree>, TreeStreet,Long> {
    private static final Long ONE = 1L;

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<TreeStreet, Long> context) {
        for(Tree t : trees){
            context.emit(new TreeStreet(neighbourhood.getName(), t.getCommon_name()),ONE);
        }
    }
}
