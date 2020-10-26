package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;

public class Query5BMapper implements Mapper<String, Long, Long, String> {
    private static final long serialVersionUID = 4330672739106556379L;

    @Override
    public void map(String neighbourhood, Long thousands, Context<Long, String> context) {
        context.emit(thousands, neighbourhood);
    }
}
