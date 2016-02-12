/**
 * Created by yezi on 2/4/16.
 */

public class Config {

    private static final String INPUT_PATH = "/home/hadoop/KDD dataset/";
    private static final String OUTPUT_PATH = "/home/hadoop/output2/";

    private Config() {}

    /*Path for DataProcessing*/
    public static String inputPathMetadata() {
        return "/home/yezi/dataset/kddCUP/kddcup.data.corrected";
    }
    public static String inputPathTestdata() {
        return "/home/yezi/dataset/kddCUP/corrected";
    }
    public static String outpuPath7att() {
        return INPUT_PATH + "KDD7att";
    }
    public static String outpuPath6att() {
        return INPUT_PATH + "KDD6att";
    }
    public static String outputPathTestdata() {
        return INPUT_PATH + "testdata";
    }
    public static String outputPathStatistic() {
        return INPUT_PATH + "statistics";
    }
    public static String pathToPlayTennis(){
        return INPUT_PATH+"playtennis.txt";
    }
    /*Path for building DTree*/
    public static String pathTo6attTrainingSet() {
        return INPUT_PATH + "KDD6att";
    }
    public static String pathTo7attTrainingSet() {
        return INPUT_PATH + "KDD7att";
    }
    public static String pathToRuleSet() {
        return "/home/hadoop/rule-6.txt";
    }
    public static String pathToOutput() {return OUTPUT_PATH + "reduceOutput";}

    /*Path for classifacation and evaluation*/
    public static String pathToTestSet(){
        return INPUT_PATH + "testdata";
    }
    public static String pathToResults(){return OUTPUT_PATH + "result";}
    public static String pathToInputSet() {
        return INPUT_PATH + "car";
    }

}
