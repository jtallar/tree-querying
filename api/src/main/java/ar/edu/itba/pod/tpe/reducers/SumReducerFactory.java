package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * Receives a key-value pair with T as key, and a Long as value.
 * Reduces only using the value and returning its sum.
 * On the constructor receives the amount of digits to be rounded.
 */
public class SumReducerFactory<T> implements ReducerFactory<T, Long, Long> {
    private static final long serialVersionUID = -8257223988384026517L;
    private final int digits;

    public SumReducerFactory(int digits) {
        this.digits = digits;
    }

    public SumReducerFactory() {
        this.digits = 0;
    }

    @Override
    public Reducer<Long, Long> newReducer(T key) {
        return new SumReducer();
    }

    private class SumReducer extends Reducer<Long, Long> {
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
            return sum - (digits > 0 ? sum % (long) (Math.pow(10, digits)) : 0);
        }
    }
}
