package ar.edu.itba.pod.tpe.mappers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.List;
import java.util.StringTokenizer;

public class TestMapper implements Mapper<Neighbourhood, List<Tree>, Neighbourhood, List<Tree>> {
    private static final Long ONE = 1L;

//    @Override
//    public void map(String key, String document, Context<String, Long> context) {
//        StringTokenizer tokenizer = new StringTokenizer(document.toLowerCase());
//        while (tokenizer.hasMoreTokens()) {
//            context.emit(tokenizer.nextToken(), ONE);
//        }
//    }

    @Override
    public void map(Neighbourhood neighbourhood, List<Tree> trees, Context<Neighbourhood, List<Tree>> context) {
        context.emit(neighbourhood, trees);
    }
}
