package ar.edu.itba.pod.tpe.reducers;

import ar.edu.itba.pod.tpe.models.TreeStreet;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class TreeStreetReducer implements ReducerFactory<TreeStreet, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(TreeStreet treeStreet) {
        return new TreeStreetCounter();
    }

    private static class TreeStreetCounter extends Reducer<Long,Long>{

        private volatile long sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
