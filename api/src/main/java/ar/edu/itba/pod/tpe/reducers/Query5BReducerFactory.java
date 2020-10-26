package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query5BReducerFactory implements ReducerFactory<Long, String, Set<String>> {

        @Override
        public Reducer<String, Set<String>> newReducer(Long thousands) {
        return new Query5BReducerFactory.Query5BReducer();
    }

        private class Query5BReducer extends Reducer<String, Set<String>> {
            private Set<String> values;

            @Override
            public void beginReduce() {
                values = new HashSet<>();
            }

            @Override
            public void reduce(String value) {
                values.add(value);
            }

            @Override
            public Set<String> finalizeReduce() {
                return values;
            }
        }
    }
