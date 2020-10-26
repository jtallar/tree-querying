package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.mappers.Query5Mapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.Query5ReducerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query5 {

    public static Map<String, Long> runQuery(Job<Neighbourhood, List<Tree>> job)
            throws InterruptedException, ExecutionException {


        final JobCompletableFuture<Map<String, Long>> future = job
                .mapper(new Query5Mapper()) // same as query 1
                .reducer(new Query5ReducerFactory())
                .submit();
        return future.get();
    }
}
