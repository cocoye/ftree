/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
//import org.apache.spark.sql.SQLContext;
import com.databricks.spark.csv.CsvParser;

public class TreeBuilder_Spark {
    public static Split currentSplit = new Split();

    public static List<Split> splitList = new ArrayList();

    public static int currentIndex = 0;
  public static void main(String[] args) {
      SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Spark Count");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    //String a="/home/yezi/data/playtennis.txt";
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
          JavaPairRDD<String, Integer> tokenized = sc.textFile(args[0]).flatMapToPair(new PairFlatMapFunction<String, String,Integer>() {
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
      /*    JavaRDD<Tuple2<String, Integer>> tokenized = sc.textFile(args[0]).
          flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
          }).reduce(new )*/
                  // /*        // count the occurrence of each word
          /*JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
                  new PairFunction<String, String, Integer>() {
                      @Override
                      public Tuple2<String, Integer> call(String s) {
                          return new Tuple2<String, Integer>(s, 1);
                      }
                  }
          ).reduceByKey(
                  new Function2<Integer, Integer, Integer>() {
                      @Override
                      public Integer call(Integer i1, Integer i2) {
                          return i1 + i2;
                      }
                  }, 1        //number of reducers = 1
          );*/
                  //tokenized.save()


          tokenized.sortByKey()
                  .saveAsHadoopFile("/home/hadoop/output2/"+currentIndex,String.class,Integer.class, TextOutputFormat.class);
          C45.getReduceResults();//将reduce输出的数组读到reduceResult[][]中
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
      System.out.println("Tree has been built!" + time + " " + System.currentTimeMillis());

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


