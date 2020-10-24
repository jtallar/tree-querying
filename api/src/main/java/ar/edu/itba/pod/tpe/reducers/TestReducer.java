package ar.edu.itba.pod.tpe.reducers;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.List;

public class TestReducer implements ReducerFactory<Neighbourhood, List<Tree>, Long> {

//    @Override
//    public Reducer<Long, Long> newReducer(String key) {
//        return new WordCountReducer();
//    }

    @Override
    public Reducer<List<Tree>, Long> newReducer(Neighbourhood neighbourhood) {
        return new TestReducerClass();
    }

    private class WordCountReducer extends Reducer<Long, Long> {
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

    private class TestReducerClass extends Reducer<List<Tree>, Long> {
        private volatile long sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(List<Tree> trees) {
            sum += trees.size();
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
