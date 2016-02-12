import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by yezi on 2/4/16.
 */
public class Classification {

    public static void main(String args[]) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> rules = env.readTextFile(Config.pathToRuleSet());
        DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

        DataSet<Tuple3<String,String,String>> results=testData.flatMap(new Classifier()).withBroadcastSet(rules, "rules");
        DataSet<String> accuracy = results.reduceGroup(new Evaluator());
        results.writeAsCsv(Config.pathToResults(), FileSystem.WriteMode.OVERWRITE);
        accuracy.print();
    }

    public static class Classifier extends RichFlatMapFunction<String, Tuple3<String,String, String>> {
        private List<String> rule = new ArrayList<String>();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            List<String> rules = getRuntimeContext().getBroadcastVariable("rules");
            for (String data : rules) {
                rule.add(data);
            }
        }

        @Override
        public void flatMap(String s, Collector<Tuple3<String,String, String>> collector) throws Exception {
            StringTokenizer itr1 = new StringTokenizer(s);
            int no_Attr = itr1.countTokens();
            String testData[] = new String[no_Attr];
            for (int i = 0; i < no_Attr; i++) {
                testData[i] = itr1.nextToken();
            }

            //System.out.println(testData[no_Attr - 1]);
            String predictClassLabel = "";
            for (int i = 0; i < rule.size(); i++) {
                StringTokenizer itr = new StringTokenizer(rule.get(i));
                int no_Rule = itr.countTokens() - 1;
                String rule[] = new String[no_Rule];
                for (int j = 0; j < no_Rule; j++) {
                    rule[j] = itr.nextToken();
                }
                String ruleClassLabel = itr.nextToken();
                // System.out.println(ruleClassLabel);
                for (int k = 0; k < no_Rule; k += 2) {
                    if (!testData[Integer.parseInt(rule[k])].equals(rule[k + 1])) {
                        break;
                    }
                    if (k != no_Rule - 2) {
                        continue;
                    }
/*                    if(k == no_Rule -2 & testData[Integer.parseInt(rule[k])].equals(rule[no_Rule-1])) {
                        predictClassLabel = ruleClassLabel;
                    }*/
                    predictClassLabel = ruleClassLabel;
                }
            }
            collector.collect(new Tuple3<String,String,String>(predictClassLabel,testData[no_Attr - 1],s));
        }
    }

    private static class Evaluator implements GroupReduceFunction<Tuple3<String,String, String>, String> {
        double correct = 0;
        double total = 0;

        @Override
        public void reduce(Iterable<Tuple3<String,String, String>> predictions, Collector<String> collector) throws Exception {
            double accuracy = 0.0;

            for(Tuple3<String,String,String> predition:predictions){
                if(predition.f0.equals(predition.f1)){
                    correct++;
                }
                total++;
            }
            accuracy=correct / total;

            collector.collect("Classifier achieved: " + accuracy*100 + " % accuracy");
        }
    }
}
