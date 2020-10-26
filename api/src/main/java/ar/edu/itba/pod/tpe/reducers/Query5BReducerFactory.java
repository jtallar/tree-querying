package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class Query5BReducerFactory implements ReducerFactory<Long, String, NavigableSet<String>> {

        @Override
        public Reducer<String, NavigableSet<String>> newReducer(Long thousands) {
        return new Query5BReducerFactory.Query5BReducer();
    }

        private class Query5BReducer extends Reducer<String, NavigableSet<String>> {
            private NavigableSet<String> values;

            @Override
            public void beginReduce() {
                values = new TreeSet<>();
            }

            @Override
            public void reduce(String value) {
                values.add(value);
            }

            @Override
            public NavigableSet<String> finalizeReduce() {
                return values;
            }
        }
    }
