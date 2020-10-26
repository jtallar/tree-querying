package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * Recieves String, Long
 * Returns  String, sum(values)
 */
public class Query5ReducerFactory implements ReducerFactory<String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(String neighbourhoodName) {
        return new Query5ReducerFactory.Query5Reducer();
    }

    private class Query5Reducer extends Reducer<Long, Long> {
        private long sum; // TODO make volatile?

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
            return sum - sum % 1000;
        }
    }
}
