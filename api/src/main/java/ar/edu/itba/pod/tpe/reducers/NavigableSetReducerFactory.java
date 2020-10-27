package ar.edu.itba.pod.tpe.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Receives a key-value pair with K as key, and a V comparable as value.
 * Reduces only using the value and a NavigableSet of them.
 */
public class NavigableSetReducerFactory<K, V extends Comparable<V> > implements ReducerFactory<K, V, NavigableSet<V>> {
    private static final long serialVersionUID = 5318965976879484481L;

    @Override
    public Reducer<V, NavigableSet<V>> newReducer(K key) {
        return new NavigableSetReducer();
    }

    private class NavigableSetReducer extends Reducer<V, NavigableSet<V>> {
        private NavigableSet<V> values;

        @Override
        public void beginReduce() {
            values = new TreeSet<>();
        }

        @Override
        public void reduce(V value) {
            values.add(value);
        }

        @Override
        public NavigableSet<V> finalizeReduce() {
            return values;
        }
    }
}
