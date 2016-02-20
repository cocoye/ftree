import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
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
    //public static Split currentSplit = new Split();

    //public static List<Split> splitList = new ArrayList<>();

    //public static int currentIndex = 0;

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long time = System.currentTimeMillis();

        Split currentSplit = new Split();
        List<Split> splitList = new ArrayList<>();
        int currentIndex = 0;

        splitList.add(currentSplit);
        //int splitListSize = splitList.size();

        //Split newSplit;

        while (splitListSize > currentIndex) {
            currentSplit = splitList.get(currentIndex);
            DataSet<Integer> currentInd = env.from.....
            DataSet<Tuple1<String>> reduceOutput = env.readTextFile(Config.pathToPlayTennis())
                    .flatMap(new MapperOne())
                    .groupBy(0)
                    .reduceGroup(new ReducerOne())
                    .sortPartition(0, Order.ASCENDING)
                    .setParallelism(1);
            DataSet<Tuple6<Integer, String, Double, Integer, String, Double>> currentNodeInformation =
                    reduceOutput.reduceGroup(new GainRatio());
            currentNodeInformation.print();

            DataSet<Integer> splitListSize = currentNodeInformation.reduceGroup(new RichGroupReduceFunction<Tuple6<Integer, String, Double, Integer, String, Double>, Integer>() {
                @Override
                public void reduce(Iterable<Tuple6<Integer, String, Double, Integer, String, Double>> iterable, Collector<Integer> collector) throws Exception {

                    Tuple6<Integer, String, Double, Integer, String, Double> tuple = iterable.iterator().next();
                    StringTokenizer attributes = new StringTokenizer(tuple.f4);
                    int splitNumber = attributes.countTokens(); //当前分裂节点属性值的个数
                    for (int i = 1; i <= splitNumber; i++) {
                        Split newSplit = new Split();
                        for (int j = 0; j < currentSplit.featureIndex.size(); j++) {
                            newSplit.featureIndex.add(currentSplit.featureIndex.get(j));
                            newSplit.featureValue.add(currentSplit.featureValue.get(j));
                        }
                        newSplit.featureIndex.add(tuple.f3);
                        newSplit.featureValue.add(attributes.nextToken());
                        //collector.collect(newSplit);
                        splitList.add(newSplit);
                        collector.collect(splitList.size());
                    }
                }
            });
            // writeAsCsv("/home/yezi/data/output/" + currentIndex, "\n", " ", FileSystem.WriteMode.OVERWRITE);

       /*    env.execute();
            C45.getReduceResults();//read reduce output information to reduceResult[][]
            entropy = C45.currentNodeEntropy();
            classLabel = C45.majorityLabel();
            currentSplit.classLabel = classLabel;

            if (entropy != 0.0 && currentSplit.featureIndex.size() != AttributeNumber) {
                bestGainRatio = 0;
                for (int i = 0; i < AttributeNumber; i++) {
                    if (!currentSplit.featureIndex.contains(i)) { //表示这个属性在当前节点下属还木有被分裂过
                        gainRatio = C45.gainRatioCalculator(i, entropy);
                        if (gainRatio >= bestGainRatio) {
                            attributeIndex = i;
                            bestGainRatio = gainRatio;
                        }
                    }
                }

                StringTokenizer attributes = new StringTokenizer(C45.getAttributeValues(attributeIndex));
                int splitNumber = attributes.countTokens(); //当前分裂节点属性值的个数

                *//**//*加上已有的分裂属性，再加上当前的分裂属性,构成一个新的分裂*//**//*
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
                *//**//*if(entropy!=0.0)
                    System.out.println("Enter rule in file:: "+rule);
                else
                    System.out.println("Enter rule in file Entropy zero ::   "+rule);*//**//*
            }
            splitListSize = splitList.size();
            System.out.println("there are " + splitListSize + " nodes.");

            currentIndex++;
        }*/
        }
        System.out.println("Tree has been built!" + time + " " + System.currentTimeMillis());
    }

    public static class MapperOne implements FlatMapFunction<String, Tuple2<String, Integer>> {
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
            String features[] = new String[featureCount];

            for (int i = 0; i < featureCount; i++) {
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

    public static class ReducerOne implements GroupReduceFunction<Tuple2<String, Integer>, Tuple1<String>> {

        int cont=0;

        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple1<String>> out) throws Exception {
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
            out.collect(new Tuple1<>(line + " " + sum));
        }
    }


    public static class GainRatio implements GroupReduceFunction<Tuple1<String>, Tuple6<Integer, String, Double,Integer,String,Double>>{
        final static int LINE_NUMBER = 10000;
        String[][] reduceResults = new String[LINE_NUMBER][4];
        int lineNumber = 0;
        int currentNode[] = new int[100];                    //或者最多分100类
        String majorityLabel = null;

        public String majorityLabel() {
            return majorityLabel;
        }
        @Override
        public void reduce(Iterable<Tuple1<String>> iterable, Collector<Tuple6<Integer, String, Double, Integer, String, Double>> collector) throws Exception {
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
            for (Tuple1<String> iter : iterable) {
                StringTokenizer itr = new StringTokenizer(iter.f0);
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

            int currentIndex = Integer.parseInt(reduceResults[0][0]);//当前属性的index
            while (currentIndex == Integer.parseInt(reduceResults[j][0])) {    // 当前索引值为reduce中输出的j行的索引值
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
                    if (!currentSplit.featureIndex.contains(i)) { //表示这个属性在当前节点下属还木有被分裂过
                        gainRatio = gainRatioCalculator(i, entropy);
                        if (gainRatio >= bestGainRatio) {
                            attributeIndex = i;
                            bestGainRatio = gainRatio;
                        }
                    }
                }
                String attributeValue = getAttributeValues(attributeIndex);
                collector.collect(new Tuple6<>(currentIndex, classLabel, entropy, attributeIndex, attributeValue,bestGainRatio));
            }else{
                String rule = "";
                for (int i = 0; i < currentSplit.featureIndex.size(); i++) {
                    rule = rule + " " + currentSplit.featureIndex.get(i) + " " + currentSplit.featureValue.get(i);
                }
                rule = rule + " " + currentSplit.classLabel;
                writeRuleToFile(rule);
            }
           // splitListSize = splitList.size();
            //System.out.println("there are " + splitListSize + " nodes.");

            currentIndex++;
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
            int s[][]=new int[1000][100];
            int sum[]=new int[1000]; //
            String currentAttributeValue="";
            double gainRatio;
            int j=0;
            int m=-1;                                         //m为分裂的索引,即同一属性属性值的个数
            int lines= lineNumber;                             //reduce 共有多少行

            for(int i=0;i<lines;i++){                        //遍历每一行reduce
                if(index ==Integer.parseInt(reduceResults[i][0])) {                //如果当前属性索引恰好为reduce某一行索引
                    if(reduceResults[i][1].contentEquals(currentAttributeValue)) {  //如果当前属性值恰好为reduce相应行的属性值
                        j++;
                        s[m][j]=Integer.parseInt(reduceResults[i][3]);      //c[m][j]为当前属性索引属性值的相应类的个数
                        sum[m]+=s[m][j];
                    } else {
                        j=0;
                        m++;
                        currentAttributeValue= reduceResults[i][1];
                        s[m][j]=Integer.parseInt(reduceResults[i][3]);
                        sum[m]=s[m][j];
                    }
                }
            }
            int i=0;
            int sumAll=0;
            while(sum[i]!=0) {
                sumAll += sum[i]; //calculating total instance in node// 计算每一个属性究竟有多少个instance
                i++;
            }
            double pEntropySum=0;
            for(int k=0;k<=m;k++) {
                double p = (double)sum[k]/sumAll;
                pEntropySum += p * entropy(s[k]);
            }
            double splitInfo= entropy(sum);
            gainRatio=(entropy-pEntropySum)/(splitInfo);
            return gainRatio;

        }
        public String getAttributeValues(int n) {
            int flag=0;
            String values="";
            String temp="";
            for(int z=0;z<LINE_NUMBER;z++){
                if(reduceResults[z][0]!=null){
                    if(n==Integer.parseInt(reduceResults[z][0])){   //如果n等于第z行reduce 输出的属性索引
                        flag=1;
                        if(!reduceResults[z][1].contentEquals(temp)) {
                            //取出reduce第z行描述的属性的属性值
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
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("home/yezi/rule.txt"), true));
            bw.write(rule);
            bw.newLine();
            bw.close();
        } catch (Exception e) {
        }
    }
}
