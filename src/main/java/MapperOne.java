import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.StringTokenizer;

public class MapperOne implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
    {
        Split split = TreeBuilder.currentSplit;
        //judge if index and value are match
        boolean flag = true;
        StringTokenizer strTokenizer = new StringTokenizer(value);

        //amount of features, here is 7
        int featureCount = strTokenizer.countTokens() - 1;

        //store features of each line
        String features[] = new String[featureCount];//0-7

        for (int i = 0; i < featureCount; i++) {//0-6
            features[i] = strTokenizer.nextToken();
        }

        String classLabel = strTokenizer.nextToken();

        int sp_size = split.featureIndex.size();//属性个数8
        //iteration according to index of each line
        for (int indexID = 0; indexID < sp_size; indexID++) {//0-7
            int currentIndexID = (Integer) split.featureIndex.get(indexID);
            String attValue = (String) split.featureValue.get(indexID);
            if (!features[currentIndexID].equals(attValue)){
                flag = false;
                break;
            }
        }
        if (flag) {
            for (int l = 0; l < featureCount; l++) {
                if (!split.featureIndex.contains(l)) {
                    //indexID,value,class,1
                    out.collect(new Tuple2<>(l + " " + features[l] + " " + classLabel, 1));
                }
            }
            if (sp_size == featureCount) {
                out.collect(new Tuple2<>(featureCount + " " + "null" + " " + classLabel, 1));
            }
        }
    }
}
