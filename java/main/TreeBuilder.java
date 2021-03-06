import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class TreeBuilder {
    public static Split currentSplit = new Split();

    public static List<Split> splitList = new ArrayList<>();

    public static int currentIndex = 0;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        System.out.println(System.currentTimeMillis());
        long time = System.currentTimeMillis();
        double entropy;
        double gainRatio;
        double bestGainRatio;
        String classLabel;
        int AttributeNumber = 6;
        int attributeIndex = 0;
        splitList.add(currentSplit);
        int splitListSize = splitList.size();
        GainRatio GR;
        Split newSplit;

        while (splitListSize > currentIndex) {
            currentSplit = splitList.get(currentIndex);
            GR = new GainRatio();
            env.readTextFile(Config.pathToInput())
                    .flatMap(new MapperOne())
                    .groupBy(0)
                    .reduceGroup(new ReducerOne())
                    .sortPartition(0, Order.ASCENDING)
                    .setParallelism(1)
                    .writeAsCsv(Config.pathToReduceOutput() + currentIndex, "\n", " ", FileSystem.WriteMode.OVERWRITE);

            env.execute();
            GR.getReduceResults();//read reduce output information to reduceResult[][]
            entropy = GR.currentNodeEntropy();
            classLabel = GR.majorityLabel();
            currentSplit.classLabel = classLabel;

            if (entropy != 0.0 && currentSplit.featureIndex.size() != AttributeNumber) {
                bestGainRatio = 0;
                for (int i = 0; i < AttributeNumber; i++) {
                    //haven't be split under this node
                    if (!currentSplit.featureIndex.contains(i)) {
                        gainRatio = GR.gainRatioCalculator(i, entropy);
                        if (gainRatio >= bestGainRatio) {
                            attributeIndex = i;
                            bestGainRatio = gainRatio;
                        }
                    }
                }

                StringTokenizer attributes = new StringTokenizer(GR.getAttributeValues(attributeIndex));
                int splitNumber = attributes.countTokens();

                /*have been split attributes and current split attributes construct a new split*/
                for (int i = 1; i <= splitNumber; i++) {
                    newSplit = new Split();
                    for (int j = 0; j < currentSplit.featureIndex.size(); j++) {
                        newSplit.featureIndex.add(currentSplit.featureIndex.get(j));
                        newSplit.featureValue.add(currentSplit.featureValue.get(j));
                    }
                    newSplit.featureIndex.add(attributeIndex);
                    newSplit.featureValue.add(attributes.nextToken());
                    splitList.add(newSplit);
                }
            } else {
                String rule = "";
                for (int i = 0; i < currentSplit.featureIndex.size(); i++) {
                    rule = rule + " " + currentSplit.featureIndex.get(i) + " " + currentSplit.featureValue.get(i);
                }
                rule = rule + " " + currentSplit.classLabel;
                writeRuleToFile(rule);
            }
            splitListSize = splitList.size();
            System.out.println("there are " + splitListSize + " nodes.");

            currentIndex++;
        }
        System.out.println("Tree has been built!" + time + " " + System.currentTimeMillis());

    }

    public static class MapperOne implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            Split split = TreeBuilder.currentSplit;
            //judge if index and value are match
            boolean flag = true;
            StringTokenizer strTokenizer = new StringTokenizer(value);

            //amount of features, here is 7
            int featureCount = strTokenizer.countTokens() - 1;

            //store features of each line
            String features[] = new String[featureCount];

            for (int i = 0; i < featureCount; i++) {
                features[i] = strTokenizer.nextToken();
            }

            String classLabel = strTokenizer.nextToken();

            int sp_size = split.featureIndex.size();
            //iteration according to index of each line
            for (int indexID = 0; indexID < sp_size; indexID++) {
                int currentIndexID = (Integer) split.featureIndex.get(indexID);
                String attValue = (String) split.featureValue.get(indexID);
                if (!features[currentIndexID].equals(attValue)) {
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

    public static class ReducerOne implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        int cont = 0;

        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
            Iterator<Tuple2<String, Integer>> iter = values.iterator();
            Tuple2<String, Integer> tuple = iter.next();
            int sum = tuple.f1;

            String line = tuple.f0.replaceAll("[()]", "");
            while (iter.hasNext()) {
                Tuple2<String, Integer> next = iter.next();
                line = next.f0.replaceAll("[()]", "");
                cont++;
                sum += tuple.f1;
            }
            out.collect(new Tuple2<>(line, sum));
        }
    }

    public static void writeRuleToFile(String rule) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.pathToRule()), true));
            bw.write(rule);
            bw.newLine();
            bw.close();
        } catch (Exception e) {
        }
    }

}
