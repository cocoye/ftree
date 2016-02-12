import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

/**
 * Created by yezi on 1/31/16.
 */
public class DataPreprocessing {
    public static final String inputPathFull = "/home/hadoop/dataset/kddCUP/corrected";
    public static final String inputPath = "/home/hadoop/dataset/kddCUP/kddcup.data_10_percent_corrected (2)";
    public static final String outputPath = "/home/hadoop/KDD dataset/full7att";
    public static final String testdataoutputPath = "/home/hadoop/KDD dataset/testdata";
    public static final String outputPath10Proz = "/home/hadoop/KDD dataset/KDDnew10prozent";
    public static final String outputPathSta = "/home/hadoop/KDD dataset/10prozent/";
    public static final String outputPathStaFull = "/home/hadoop/KDD dataset/Full/";
    public static final String outputPath10ProzDH = "/home/hadoop/KDD dataset/KDDnew10prozentdouhao";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*变成6个属性，一个class*/
        env.readCsvFile(Config.outpuPath7att()).fieldDelimiter(" ").includeFields("11111111")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(new MapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple7<String, String, String, String, String, String, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, String, String> map(Tuple8<String, String, String, String, String, String, String, String> value) throws Exception {
                        return new Tuple7<>(value.f0, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7);
                    }
                })
                .setParallelism(1).writeAsCsv(Config.outpuPath6att(), "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
        env.execute();


        /*format source dataset*/
        env.readCsvFile(Config.inputPathMetadata()).fieldDelimiter(",").includeFields("011110100001000000000010000000000000000001")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class).flatMap(new FlatMapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple8<String, String, String, String, String, String, String, String>>() {
            @Override
            public void flatMap(Tuple8<String, String, String, String, String, String, String, String> value, Collector<Tuple8<String, String, String, String, String, String, String, String>> out) throws Exception {
                if (value.f7.compareTo("normal.") == 0) {
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "normal"));
                } else {
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "unnormal"));
                }
            }
        }).setParallelism(1).writeAsCsv(testdataoutputPath, "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
        env.execute();



        /*format attributes*/
        env.readCsvFile(Config.inputPathMetadata()).fieldDelimiter(",").includeFields("011110100001000000000010000000000000000001")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(value.f7.compareTo("normal.")==0){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "normal"));
                }
                else {
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, "unnormal"));
                }
            }}).map(new MapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public Tuple8<String, String, String, String, String, String, String, String> map(Tuple8<String, String, String, String, String, String, String, String> value) throws Exception {
                if(value.f5.compareTo("0")==0) {
                    return new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, "suc", value.f6, value.f7);
                }
                return new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, "insuc", value.f6, value.f7);
            }
        }).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(Integer.parseInt(value.f6)>=100&&Integer.parseInt(value.f6)<=300){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, "200", value.f7));
                }
                if (Integer.parseInt(value.f6)>=300){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, "511", value.f7));
                }
                if(Integer.parseInt(value.f6)<=100){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, "0", value.f7));}
            }}).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(Integer.parseInt(value.f3)==0){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "0", value.f4, value.f5, value.f6, value.f7));
                }
                if (Integer.parseInt(value.f3)<=200&&Integer.parseInt(value.f3)>0){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "105", value.f4, value.f5, value.f6, value.f7));
                }
                if(Integer.parseInt(value.f3)<=600&&Integer.parseInt(value.f3)>200){out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "520", value.f4, value.f5, value.f6, value.f7));}
                else{out.collect(new Tuple8<>(value.f0, value.f1, value.f2, "1032", value.f4, value.f5, value.f6, value.f7));}
            }}).flatMap(new FlatMapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public void flatMap(Tuple8< String, String, String, String, String, String, String, String> value, Collector<Tuple8< String, String, String, String, String, String, String, String>> out) throws Exception {
                if(Integer.parseInt(value.f4)==0){
                    out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, "same", value.f5, value.f6, value.f7));
                }
                else { out.collect(new Tuple8<>(value.f0, value.f1, value.f2, value.f3, "diff", value.f5, value.f6, value.f7));}
            }}).setParallelism(1).writeAsCsv(testdataoutputPath, "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
        env.execute();



        /* 处理后的data format(逗号与空格分割转换)*/
        env.readCsvFile(outputPath).fieldDelimiter(",").includeFields("11111111")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .setParallelism(1).writeAsCsv(outputPath+"kongge", "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

        /*Dataset info statistics*/
        env.readCsvFile(outputPath10ProzDH).fieldDelimiter(",").includeFields("10000000").types(String.class).map(new MapFunction<Tuple1<String>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple1<String> value) throws Exception {
                return new Tuple2<String, Integer>(value.f0,1);
            }
        }).groupBy(0).sum(1).setParallelism(1).writeAsCsv(outputPathSta+"field1", "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
        env.execute();

        env.readCsvFile(Config.pathToInputSet()).fieldDelimiter(",").includeFields("1111111")
                .types(String.class, String.class, String.class,String.class,String.class,String.class, String.class)
                .map(new MapFunction<Tuple7<String, String, String, String, String, String, String>, Tuple7<Long, Long, Long, Long, Long, Long, Long>>() {
                    @Override
                    public Tuple7<Long, Long, Long, Long, Long, Long, Long> map(Tuple7<String, String, String, String, String, String, String> tuple7) throws Exception {
                        Long a0=0L, a1=0L, a2=0L, a3=0L, a4=0L, a5=0L, c=0L;
                        switch (tuple7.f0) {
                            case "vhigh":
                                a0 = 0L;
                                break;
                            case "high":
                                a0 = 1L;
                                break;
                            case "med":
                                a0 = 2L;
                                break;
                            case "low":
                                a0 = 3L;
                                break;
                        }
                        switch (tuple7.f1) {
                            case "vhigh":
                                a1 = 0L;
                                break;
                            case "high":
                                a1 = 1L;
                                break;
                            case "med":
                                a1 = 2L;
                                break;
                            case "low":
                                a1 = 3L;
                                break;
                        }
                        switch (tuple7.f2) {
                            case "2":
                                a2 = 2L;
                                break;
                            case "3":
                                a2 = 3L;
                                break;
                            case "4":
                                a2 = 4L;
                                break;
                            case "5more":
                                a2 = 5L;
                                break;
                        }
                        switch (tuple7.f3) {
                            case "2":
                                a3 = 2L;
                                break;
                            case "3":
                                a3 = 3L;
                                break;
                            case "4":
                                a3 = 4L;
                                break;
                            case "more":
                                a3 = 5L;
                                break;
                        }
                        switch (tuple7.f4) {
                            case "big":
                                a4 = 0L;
                                break;
                            case "med":
                                a4 = 1L;
                                break;
                            case "small":
                                a4 = 2L;
                                break;
                        }
                        switch (tuple7.f5) {
                            case "high":
                                a5 = 0L;
                                break;
                            case "med":
                                a5 = 1L;
                                break;
                            case "low":
                                a5 = 2L;
                                break;
                        }
                        switch (tuple7.f6) {
                            case "unacc":
                                c = 0L;
                                break;
                            case "acc":
                                c = 1L;
                                break;
                            case "good":
                                c = 2L;
                                break;
                            case "vgood":
                                c = 3L;
                                break;
                        }



                        return new Tuple7<>(c,a0, a1, a2, a3, a4, a5);
                    }
                }).setParallelism(1).writeAsCsv("/home/yezi/decisiontrees/sparktree/carforspark.csv", "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }



/*Using Data Description*/
/*1. Number of Instances: 625 (1306263 normal, 5051220 unnormal)

6. Number of Attributes: 7 (categorical) + class name = 8

7. Attribute Information:
	1. protocol_type: 3 (icmp  2858953;
	                    tcp   3110599;
	                    udp   387931)
	2. service: 70 (IRC 715,X11 167,Z39_50 2162,aol 4,auth 6772,bgp 2108,courier 2056,csnet_ns 2114,
	                ctf 2144,daytime 2124,discard 2129,domain 2242,domain_u 115662,echo 2124,eco_i 32678
                    ecr_i 2815195,efs 2096,exec 2114,finger 13795,ftp 6360,ftp_data 48103,gopher 2162,harvest 4,
                    hostnames 2118,http 738707,http_2784 2,http_443 2096,http_8001 4,imap4 2150,iso_tsap 2114,
                    klogin 2122,kshell 2094,ldap 2096,link 2158,login 2106,mtp 2164,name 2150,netbios_dgm 2118,
                    netbios_ns 2122,netbios_ssn 2126,netstat 2120,nnsp 2090,nntp 2140,ntp_u 7666,other 144352,
                    pm_dump 10,pop_2 2122,pop_3 3963,printer 2106,private 2212324,red_i 18,remote_job 2156,rje 2152,
                    shell 2107,smtp 98255,sql_net 2118,ssh 2153,sunrpc 2122,supdup 2124,systat 2132,telnet 7046,
                    tftp_u 6,tim_i 13,time 3168,urh_i 296,urp_i 10753,uucp 2090,uucp_path 2130,vmnet 2116,whois 2158)
	3. flag: 11 (OTH 110,REJ 540261,RSTO 10677,RSTOS0 241,
                 RSTR 15799,S0 1748605,S1 668,S2 211,S3 60,
                 SF 4038771,SH 2080)
	4. src_bytes: 4 (0 1158308,1032 3857667
                     105 294906,520 1046602)
	5. land: 2 (diff 56,same 6357427)
	6. logged_in: 2 (insuc 771337, suc 5586146)
	7. Count(number of connections to the same host as the current connection): 3 (0 1452193,200 2067480,511 2837810)

8. Missing Attribute Values:
	none

9. Class Distribution:
   1. 46.08 percent are L
   2. 07.84 percent are B
   3. 46.08 percent are R
2.  protocol_type: symbolic.
3.  service: symbolic.
4.  flag: symbolic .0 normal,1
5.  src_bytes  	number of data bytes from source to destination 520,1032,0
7.  land: symbolic.
12. logged_in: symbolic.
23. Count  	number of connections to the same host as the current connection in the past two seconds 100-300：200,511，0


The ``same host'' features examine only the connections in the past two seconds that have the same destination host as
the current connection, and calculate statistics related to protocol behavior, service, etc.The similar ``same service''
features examine only the connections in the past two seconds that have the same service as the current connection.
"Same host" and "same service" features are together called  time-based traffic features of the connection records.
*/
}