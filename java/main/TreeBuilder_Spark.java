import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import com.databricks.spark.csv.CsvParser;

public class TreeBuilder_Spark {
    public static Split currentSplit = new Split();

    public static List<Split> splitList = new ArrayList();

    public static int currentIndex = 0;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Spark Tree");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
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
        GainRatio C45;
        Split newSplit;
        while (splitListSize > currentIndex) {
            currentSplit = splitList.get(currentIndex);
            C45 = new GainRatio();
            // split each document into words
            JavaPairRDD<String, Integer> tokenized = sc.textFile(Config.pathToInput()).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                @Override
                public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                    List<Tuple2<String, Integer>> b = new ArrayList<Tuple2<String, Integer>>();
                    Split split = currentSplit;
                    int sp_size = 0;
                    boolean flag = true;
                    StringTokenizer strTokenizer = new StringTokenizer(s);
                    int featureCount = strTokenizer.countTokens() - 1;
                    String features[] = new String[featureCount];
                    for (int i = 0; i < featureCount; i++) {
                        features[i] = strTokenizer.nextToken();
                    }
                    String classLabel = strTokenizer.nextToken();
                    sp_size = split.featureIndex.size();
                    for (int indexID = 0; indexID < sp_size; indexID++) {
                        int currentIndexID = (Integer) split.featureIndex.get(indexID);
                        String attValue = (String) split.featureValue.get(indexID);
                        if (!features[currentIndexID].equals(attValue)) {
                            flag = false;
                            break;
                        }
                    }
                    Tuple2<String, Integer> a = new Tuple2<String, Integer>("a", 1);
                    if (flag) {
                        for (int l = 0; l < featureCount; l++) {
                            if (!split.featureIndex.contains(l)) {
                                //indexID,value,class,1
                                a = new Tuple2<String, Integer>(l + " " + features[l] + " " + classLabel, 1);
                                b.add(a);
                            }

                        }
                        if (sp_size == featureCount) {
                            a = new Tuple2<String, Integer>(featureCount + " " + "null" + " " + classLabel, 1);
                            b.add(a);
                        }
                    }
                    return b;
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            }, 1);
            tokenized.sortByKey()
                    .saveAsHadoopFile(Config.pathToReduceOutput() + currentIndex, String.class, Integer.class, TextOutputFormat.class);
            C45.getReduceResults();
            entropy = C45.currentNodeEntropy();
            classLabel = C45.majorityLabel();
            currentSplit.classLabel = classLabel;

            if (entropy != 0.0 && currentSplit.featureIndex.size() != AttributeNumber) {
                bestGainRatio = 0;
                for (int i = 0; i < AttributeNumber; i++) {
                    if (!currentSplit.featureIndex.contains(i)) {
                        gainRatio = C45.gainRatioCalculator(i, entropy);
                        if (gainRatio >= bestGainRatio) {
                            attributeIndex = i;
                            bestGainRatio = gainRatio;
                        }
                    }
                }

                StringTokenizer attributes = new StringTokenizer(C45.getAttributeValues(attributeIndex));
                int splitNumber = attributes.countTokens();
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


