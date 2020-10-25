package ar.edu.itba.pod.tpe.reducers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * Recieves String, Long
 * Returns  String, sum(values)
 */
public class Query1ReducerFactory implements ReducerFactory<Neighbourhood, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(Neighbourhood neighbourhoodName) {
        return new Query1ReducerFactory.Query1Reducer();
    }

    private class Query1Reducer extends Reducer<Long, Long> {
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
            return sum;
        }
    }
}
