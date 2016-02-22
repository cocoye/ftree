public class Config {

    private static final String INPUT_PATH = "/home/hadoop/KDD dataset/";
    private static final String OUTPUT_PATH = "/home/hadoop/output2/";

    private Config() {
    }

    /*Path for DataProcessing*/
    public static String inputPathMetadata() {
        return "/home/hadoop/dataset/kddCUP/kddcup.data.corrected";
    }

    public static String inputPathTestdata() {
        return "/home/hadoop/dataset/kddCUP/corrected";
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

    public static String pathToInput() {
        return INPUT_PATH + "playtennis.txt";
    }

    /*Path for building DTree*/

    public static String pathToRule() {
        return "/home/hadoop/rule.txt";
    }

    public static String pathToReduceOutput() {
        return OUTPUT_PATH + "reduceOutput";
    }

    /*Path for classifacation and evaluation*/
    public static String pathToTestSet() {
        return INPUT_PATH + "testdata";
    }

    public static String pathToResults() {
        return OUTPUT_PATH + "result";
    }
}
