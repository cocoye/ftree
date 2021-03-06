import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class TreeBuilder1 {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long time = System.currentTimeMillis();

        DataSet<Split> initial1 = env.fromElements(new Split());
        DataSet<Integer> initial2 = env.fromElements(0);
        DataSet<Tuple4<Split, List, Integer, Boolean>> initial = initial1.cross(initial2).with(new CrossFunction<Split, Integer, Tuple4<Split, List, Integer, Boolean>>() {
            @Override
            public Tuple4<Split, List, Integer, Boolean> cross(Split currentSplit, Integer currentindex) throws Exception {
                List<Split> splitList = new ArrayList<>();
                splitList.add(currentSplit);
                boolean b = (splitList.size() > currentindex);
                return new Tuple4<Split, List, Integer, Boolean>(currentSplit, splitList, currentindex, b);
            }
        }).iterate(1000000);
        DataSet<String> input = env.readTextFile(Config.pathToInput());

        IterativeDataSet<Boolean> split = input
                .flatMap(new MapperOne())
                .withBroadcastSet(initial, "initial")
                .groupBy(0)
                .reduceGroup(new ReducerOne())
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1)
                .reduceGroup(new GainRatio()).iterate(1000000);

        DataSource<Boolean> close = env.fromElements(false);
        split.closeWith(close);
        System.out.println("Tree has been built!" + time + " " + System.currentTimeMillis());
    }
    public static class MapperOne extends RichFlatMapFunction<String, Tuple5<String, Integer,Split,List<Split>,Integer>> {
      private Split currentSplit = new Split();

     private List<Split> splitList = new ArrayList<>();
        private int currentIndex = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            List<Tuple4<Split,List,Integer,Boolean>> delta = getRuntimeContext().getBroadcastVariable("initial");
            for(Tuple4<Split,List,Integer,Boolean> tuple : delta) {
                currentSplit=tuple.f0;
                splitList.add(currentSplit);
                currentIndex = tuple.f2;
            }
        }

        @Override
        public void flatMap(String value, Collector<Tuple5<String, Integer,Split,List<Split>,Integer>> out) throws Exception
        {

            currentSplit = splitList.get(currentIndex);
            boolean flag = true;
            StringTokenizer strTokenizer = new StringTokenizer(value);

            int featureCount = strTokenizer.countTokens() - 1;

            String features[] = new String[featureCount];

            for (int i = 0; i < featureCount; i++) {
                features[i] = strTokenizer.nextToken();
            }

            String classLabel = strTokenizer.nextToken();

            int sp_size = currentSplit.featureIndex.size();
            for (int indexID = 0; indexID < sp_size; indexID++) {
                int currentIndexID = (Integer) currentSplit.featureIndex.get(indexID);
                String attValue = (String) currentSplit.featureValue.get(indexID);
                if (!features[currentIndexID].equals(attValue)){
                    flag = false;
                    break;
                }
            }

            if (flag) {
                for (int l = 0; l < featureCount; l++) {
                    if (!currentSplit.featureIndex.contains(l)) {
                        out.collect(new Tuple5<>(l + " " + features[l] + " " + classLabel, 1,currentSplit,splitList,currentIndex));
                    }
                }
                if (sp_size == featureCount) {
                    out.collect(new Tuple5<>(featureCount + " " + "null" + " " + classLabel, 1,currentSplit,splitList,currentIndex));
                }
            }
        }
    }

    public static class ReducerOne implements GroupReduceFunction<Tuple5<String, Integer,Split,List<Split>,Integer>, Tuple4<String,Split,List<Split>,Integer>> {

        int cont=0;

        @Override
        public void reduce(Iterable<Tuple5<String, Integer,Split,List<Split>,Integer>> values, Collector<Tuple4<String,Split,List<Split>,Integer>> out) throws Exception {
            Iterator<Tuple5<String, Integer,Split,List<Split>,Integer>> iter = values.iterator();
            Tuple5<String, Integer,Split,List<Split>,Integer> tuple = iter.next();
            int sum=tuple.f1;

            String line = tuple.f0.replaceAll("[()]", "");
            while (iter.hasNext()) {
                Tuple5<String, Integer,Split,List<Split>,Integer> next = iter.next();
                line=next.f0.replaceAll("[()]", "");
                cont ++;
                sum += tuple.f1;
            }
            out.collect(new Tuple4<>(line + " " + sum,tuple.f2,tuple.f3,tuple.f4));
        }
    }


    public static class GainRatio implements GroupReduceFunction<Tuple4<String,Split,List<Split>,Integer>, Boolean>{
        final static int LINE_NUMBER = 10000;
        String[][] reduceResults = new String[LINE_NUMBER][4];
        int lineNumber = 0;
        int currentNode[] = new int[100];                  
        String majorityLabel = null;

        public String majorityLabel() {
            return majorityLabel;
        }
        @Override
        public void reduce(Iterable<Tuple4<String,Split,List<Split>,Integer>> iterable, Collector<Boolean> collector) throws Exception {
            double entropy;
            String classLabel;
            int labelMark[] = new int[LINE_NUMBER];
            int j = 0;
            int tempIndex = -1;
            int maxNumber = 0;
            double gainRatio;
            double bestGainRatio;
            int attributeIndex = 0;
            int AttributeNumber = 4;
            Tuple4<String,Split,List<Split>,Integer> currentTuple = iterable.iterator().next();
            Split currentSplit = currentTuple.f1;
            for (Tuple4<String,Split,List<Split>,Integer> iter : iterable) {
                StringTokenizer itr = new StringTokenizer(iter.f0.toString());
                reduceResults[lineNumber][0] = itr.nextToken();
                reduceResults[lineNumber][1] = itr.nextToken();
                reduceResults[lineNumber][2] = itr.nextToken();
                reduceResults[lineNumber][3] = itr.nextToken();
                lineNumber++;
            }
            reduceResults[lineNumber][0] = null;
            reduceResults[lineNumber][1] = null;
            reduceResults[lineNumber][2] = null;
            reduceResults[lineNumber][3] = null;

            int currentIndex = Integer.parseInt(reduceResults[0][0]);
            while (currentIndex == Integer.parseInt(reduceResults[j][0])) {    
                if (labelMark[j] == 0) {
                    classLabel = reduceResults[j][2];
                    tempIndex++;
                    int i = j;
                    while (currentIndex == Integer.parseInt(reduceResults[i][0])) {
                        if (labelMark[i] == 0) {
                            if (classLabel.contentEquals(reduceResults[i][2])) {
                                currentNode[tempIndex] += Integer.parseInt(reduceResults[i][3]);
                                labelMark[i] = 1;
                            }
                        }
                        i++;
                        if (i == lineNumber)
                            break;
                    }
                    if (currentNode[tempIndex] > maxNumber) {
                        maxNumber = currentNode[tempIndex];
                        majorityLabel = classLabel;
                    }
                    System.out.print("currentNodeValue: " + currentNode[tempIndex] + "\n" + "classLabel:" + classLabel + "\n");
                } else {
                    j++;
                }
                if (j == lineNumber)
                    break;
            }

            entropy = entropy(currentNode);
            classLabel = this.majorityLabel();
            if (entropy != 0.0 && currentSplit.featureIndex.size() != AttributeNumber) {
                bestGainRatio = 0;
                for (int i = 0; i < AttributeNumber; i++) {
                    if (!currentSplit.featureIndex.contains(i)) { 
                        gainRatio = gainRatioCalculator(i, entropy);
                        if (gainRatio >= bestGainRatio) {
                            attributeIndex = i;
                            bestGainRatio = gainRatio;
                        }
                    }
                }
                String attributeValue = getAttributeValues(attributeIndex);
                StringTokenizer attributes = new StringTokenizer(attributeValue);
                int splitNumber = attributes.countTokens(); 
                for (int i = 1; i <= splitNumber; i++) {
                    Split newSplit = new Split();
                    for (int l = 0; l < currentSplit.featureIndex.size(); l++) {
                        newSplit.featureIndex.add(currentSplit.featureIndex.get(l));
                        newSplit.featureValue.add(currentSplit.featureValue.get(l));
                    }
                    newSplit.featureIndex.add(attributeIndex);
                    newSplit.featureValue.add(attributes.nextToken());

                   currentTuple.f2.add(newSplit);
                }
            }else{
                String rule = "";
                for (int i = 0; i < currentSplit.featureIndex.size(); i++) {
                    rule = rule + " " + currentSplit.featureIndex.get(i) + " " + currentSplit.featureValue.get(i);
                }
                rule = rule + " " + currentSplit.classLabel;
                writeRuleToFile(rule);
            }

            currentTuple.f3 ++;
            boolean b = currentTuple.f3 > currentTuple.f2.size();
            collector.collect(b);

        }

        double entropy(int c[]) {
            double entropy = 0;

            int i = 0, j = 0;
            int sum = 0;
            double p;
            while (c[i] != 0) {
                sum += c[i];
                i++;
            }
            while (c[j] != 0) {
                p = (double) c[j] / sum;
                entropy += -(p * (Math.log(p) / Math.log(2)));
                j++;
            }
            return entropy;
        }

        double gainRatioCalculator(int index, double entropy)
        {
            //100 is considered as max ClassLabels
            int s[][] = new int[LINE_NUMBER][100];
            int sum[] = new int[LINE_NUMBER]; //
            String currentAttributeValue = "";
            double gainRatio;
            int j = 0;
            /*amount of feature' values with the same feature*/
            int m = -1;
            int lines = lineNumber;

            for (int i = 0; i < lines; i++) {
            /*current featureIndex is featureIndex of reduceOutput in this column*/
                if (index == Integer.parseInt(reduceResults[i][0])) {
                /*current featureValue is featureValue of reduceOutput in this column*/
                    if (reduceResults[i][1].contentEquals(currentAttributeValue)) {
                        j++;
                        //c[m][j] is the amount of class of current value
                        s[m][j] = Integer.parseInt(reduceResults[i][3]);
                        sum[m] += s[m][j];
                    } else {
                        j = 0;
                        m++;
                        currentAttributeValue = reduceResults[i][1];
                        s[m][j] = Integer.parseInt(reduceResults[i][3]);
                        sum[m] = s[m][j];
                    }
                }
            }
            int i = 0;
            int sumAll = 0;
        /*calculate the amount of instances of each attribute*/
            while (sum[i] != 0) {
                sumAll += sum[i];
                i++;
            }
            double pEntropySum = 0;
            for (int k = 0; k <= m; k++) {
                double p = (double) sum[k] / sumAll;
                pEntropySum += p * entropy(s[k]);
            }
            double splitInfo = entropy(sum);
            gainRatio = (entropy - pEntropySum) / (splitInfo);
            return gainRatio;

        }
        public String getAttributeValues(int n) {
            int flag=0;
            String values="";
            String temp="";
            for(int z=0;z<LINE_NUMBER;z++){
                if(reduceResults[z][0]!=null){
                    if(n==Integer.parseInt(reduceResults[z][0])){
                        flag=1;
                        if(!reduceResults[z][1].contentEquals(temp)) {
                            values=values+" "+ reduceResults[z][1];
                            temp= reduceResults[z][1];
                        }
                    }else{
                        if(flag==1)
                            break;
                    }
                }else
                    break;
            }
            return values;

        }
    }

    public static void writeRuleToFile(String rule) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.pathToReduceOutput()), true));
            bw.write(rule);
            bw.newLine();
            bw.close();
        } catch (Exception e) {
        }
    }
}
