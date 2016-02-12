import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class TreeBuilder {
    public static Split currentSplit = new Split();

    public static List<Split> splitList = new ArrayList<>();

    public static int currentIndex = 0;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        double entropy;
        double gainRatio;
        double bestGainRatio;
        String classLabel;
        int AttributeNumber = 4;
        int attributeIndex = 0;
        splitList.add(currentSplit);
        int splitListSize = splitList.size();
        GainRatio gainObj;
        Split newSplit;

        while (splitListSize > currentIndex) {
            currentSplit = splitList.get(currentIndex);
            gainObj = new GainRatio();
            env.readTextFile(Config.outpuPath7att())
                    .flatMap(new MapperOne())
                    .groupBy(0)
                    .reduceGroup(new ReducerOne())
                    .sortPartition(0, Order.ASCENDING)
                    .setParallelism(1)
                    .writeAsCsv(Config.pathToOutput() + currentIndex, "\n", " ", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1);
            env.execute();
            gainObj.getReduceResults();//将reduce输出的数组读到count[][]中
            entropy = gainObj.currentNodeEntropy();
            classLabel = gainObj.majorityLabel();
            currentSplit.classLabel = classLabel;

            if (entropy != 0.0 && currentSplit.featureIndex.size() != AttributeNumber) {
                bestGainRatio = 0;
                for (int i = 0; i < AttributeNumber; i++) {        //Finding the gain of each attribute
                    if (!currentSplit.featureIndex.contains(i)) { //表示这个属性在当前节点下属还木有被分裂过
                        gainRatio = gainObj.gainRatioCalculator(i, entropy);
                        if (gainRatio >= bestGainRatio) {
                            attributeIndex = i;
                            bestGainRatio = gainRatio;
                        }
                    }
                }

                StringTokenizer attributes = new StringTokenizer(gainObj.getAttributeValues(attributeIndex));
                int splitNumber = attributes.countTokens(); //当前分裂节点属性值的个数

                /*加上已有的分裂属性，再加上当前的分裂属性,构成一个新的分裂*/
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
                /*if(entropy!=0.0)
                    System.out.println("Enter rule in file:: "+rule);
                else
                    System.out.println("Enter rule in file Entropy zero ::   "+rule);*/
            }
            splitListSize = splitList.size();
            System.out.println("there are " + splitListSize + " nodes.");

            currentIndex++;
        }
        System.out.println("Tree has been built!");
    }

    public static void writeRuleToFile(String rule) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.pathToRuleSet()), true));
            bw.write(rule);
            bw.newLine();
            bw.close();
        } catch (Exception e) {
        }
    }

}
