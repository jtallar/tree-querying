package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


/**
 * Recieves String, Long
 * Returns  String, sum(values)
 */
public class Query4ReducerFactory implements ReducerFactory<String, Long, Long> {
    private static final long serialVersionUID = -4474187647081106625L;

    @Override
    public Reducer<Long, Long> newReducer(String neighbourhoodName) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<Long, Long> {
        private long sum;

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
