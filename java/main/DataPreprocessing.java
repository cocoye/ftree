import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class DataPreprocessing {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /*format source dataset*/
        env.readCsvFile(Config.inputPathMetadata()).fieldDelimiter(",").includeFields("011110100001000000000010000000000000000001")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class).flatMap(new FlatMapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple8<String, String, String, String, String, String, String, String>>() {
            @Override
            public void flatMap(Tuple8<String, String, String, String, String, String, String, String> value, Collector<Tuple8<String, String, String, String, String, String, String, String>> out) throws Exception {
                if (value.f7.compareTo("normal.") == 0) {
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "normal"));
                } else {
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "unnormal"));
                }
            }
        }).setParallelism(1).writeAsCsv(Config.inputPathTestdata(), "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
        env.execute();

        /*format attributes*/
        env.readCsvFile(Config.inputPathMetadata()).fieldDelimiter(",").includeFields("011110100001000000000010000000000000000001")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(value.f7.compareTo("normal.")==0){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "normal"));
                } else {out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "unnormal"));}
            }}).map(new MapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public Tuple8<String, String, String, String, String, String, String, String> map(Tuple8<String, String, String, String, String, String, String, String> value) throws Exception {
                if(value.f5.compareTo("0")==0) {return new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, "suc", value.f6, value.f7);}
                return new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, "insuc", value.f6, value.f7);
            }
        }).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(Integer.parseInt(value.f6)>=100&&Integer.parseInt(value.f6)<=300){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, "200", value.f7));}
                if (Integer.parseInt(value.f6)>=300){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, "511", value.f7));}
                if(Integer.parseInt(value.f6)<=100){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, "0", value.f7));}
            }}).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(Integer.parseInt(value.f3)==0){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "0", value.f4, value.f5, value.f6, value.f7));}
                if (Integer.parseInt(value.f3)<=200&&Integer.parseInt(value.f3)>0){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "105", value.f4, value.f5, value.f6, value.f7));}
                if(Integer.parseInt(value.f3)<=600&&Integer.parseInt(value.f3)>200){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "520", value.f4, value.f5, value.f6, value.f7));}
                else{out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "1032", value.f4, value.f5, value.f6, value.f7));}
            }}).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(Integer.parseInt(value.f4)==0){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, "same", value.f5, value.f6, value.f7));}
                else { out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, "diff", value.f5, value.f6, value.f7));}
            }}).setParallelism(1).writeAsCsv(Config.inputPathTestdata(), "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }
}