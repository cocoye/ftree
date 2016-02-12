import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by hadoop on 31.01.16.
 */
public class ReducerOne implements org.apache.flink.api.common.functions.GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    @Override
    public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
        int cont = 1;
        Iterator<Tuple2<String, Integer>> iter = values.iterator();
        Tuple2<String, Integer> key = iter.next();

        String line = key.f0.replaceAll("[()]", "");
        int sum=key.f1;
        StringTokenizer itr = new StringTokenizer(line);
        while (iter.hasNext()) {
            Tuple2<String, Integer> next = iter.next();
            line=next.f0.replaceAll("[()]", "");
            cont++;
            sum += key.f1;
        }
        out.collect(new Tuple2<String, Integer>(line,sum));
    }


}
