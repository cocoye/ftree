import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


import java.util.Iterator;

/**
 * Created by hadoop on 31.01.16.
 */
public class ReducerOne implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    int cont=0;

    @Override
    public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
        Iterator<Tuple2<String, Integer>> iter = values.iterator();
        Tuple2<String, Integer> tuple = iter.next();
        int sum=tuple.f1;

        String line = tuple.f0.replaceAll("[()]", "");
        while (iter.hasNext()) {
            Tuple2<String, Integer> next = iter.next();
            line=next.f0.replaceAll("[()]", "");
            cont ++;
            sum += tuple.f1;
        }
        out.collect(new Tuple2<>(line, sum));
    }
}
