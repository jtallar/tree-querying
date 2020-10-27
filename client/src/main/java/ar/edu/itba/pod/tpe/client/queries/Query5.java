package ar.edu.itba.pod.tpe.client.queries;

import ar.edu.itba.pod.tpe.mappers.NeighbourhoodTreeMapper;
import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import ar.edu.itba.pod.tpe.reducers.SumReducerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query5 {

    public static Map<String, Long> runQuery(Job<Neighbourhood, List<Tree>> job)
            throws InterruptedException, ExecutionException {

        final JobCompletableFuture<Map<String, Long>> future = job
                .mapper(new NeighbourhoodTreeMapper())
                .reducer(new SumReducerFactory<>(3))
                .submit();
        return future.get();
    }
}
