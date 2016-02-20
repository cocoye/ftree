/**
 * Created by yezi on 2/4/16.
 */

public class Config {

    private static final String INPUT_PATH = "/home/yezi/data/KDD dataset/";
    private static final String OUTPUT_PATH = "/home/yezi/output2/";
    private static final String DATAGE_PATH = "/home/yezi/data/1/";

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
        return INPUT_PATH + "kdddelete2";
    }
    public static String pathTo7attTrainingSet() {
        return INPUT_PATH + "testdata";
    }
    public static String pathToRuleSet() {
        return "/home/yezi/rule.txt";
    }
    public static String pathToOutput() {return OUTPUT_PATH + "reduceOutput";}

    /*Path for classifacation and evaluation*/
    public static String pathToTestSet(){
        return INPUT_PATH + "";
    }
    public static String pathToResults(){return OUTPUT_PATH + "result";}
    public static String pathToInputSet() {
        return INPUT_PATH + "/kdddelete2.txt";
    }
    public static String pathToGeSet(){
        return DATAGE_PATH ;
    }
    public static String pathToGeTestSet(){
        return DATAGE_PATH + "2.txt";
    }

}
