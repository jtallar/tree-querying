package ar.edu.itba.pod.tpe.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Given a key-value pair, with the neighbourhood name as key and a count of trees as value,
 * emits the inverted pair to a value-key pair. Only if the condition is met.
 */
public class InverterMapper implements Mapper<String, Long, Long, String> {
    private static final long serialVersionUID = 4330672739106556379L;

    @Override
    public void map(String neighbourhood, Long thousands, Context<Long, String> context) {
        if (thousands > 0) context.emit(thousands, neighbourhood);
    }
}
