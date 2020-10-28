package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * Receives a key-value pair with T as key, and a Double as value.
 * Reduces only using the value and returning its sum.
 */
public class SumAvgReducerFactory<T> implements ReducerFactory<T, Double, Double> {
    private static final long serialVersionUID = -3474187647081106625L;

    @Override
    public Reducer<Double, Double> newReducer(T key) {
        return new SumAvgReducer();
    }

    private class SumAvgReducer extends Reducer<Double, Double> {
        private double sum;
        private int total;

        @Override
        public void beginReduce() {
            sum = 0;
            total = 0;
        }

        @Override
        public void reduce(Double value) {
            sum += value;
            total++;
        }

        @Override
        public Double finalizeReduce() {
            return sum/total;
        }
    }
}
