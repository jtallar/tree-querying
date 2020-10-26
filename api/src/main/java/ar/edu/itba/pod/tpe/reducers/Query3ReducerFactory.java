package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


/**
 * Recieves String, Double
 * Returns  String, avg(values)
 */
public class Query3ReducerFactory implements ReducerFactory<String, Double, Double> {
    private static final long serialVersionUID = -3474187647081106625L;

    @Override
    public Reducer<Double, Double> newReducer(String treeName) {
        return new Query3Reducer();
    }

    private class Query3Reducer extends Reducer<Double, Double> {
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
