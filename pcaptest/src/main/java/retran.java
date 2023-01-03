//参数如下：输入文件 输出的excel 时间间隔选择 输出重传包的临界值n
// "src/file.seq" "/home/yaozhou/IdeaProjects/1.csv"  1 10
//目前问题：输出间隙太宽导致空白数据很多，且对于file.seq大概到excel的1200行开始才有集中的数据呈现
//待实现功能：大于n的重传包输出
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
//import scala.Tuple2;
//import scala.Tuple3;
import scala.Tuple2;
//import scala.collection.Iterator;
//import scala.collection.immutable.List;

import java.io.File;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class retran {
    public static String stp2time(double stp, int para) {
        String[] format = {"HH:mm:ss", "HH:mm"};
        int[] ex = {2, 3};
        SimpleDateFormat sdf = new SimpleDateFormat(format[para]);
        long milliSec = (long) Math.floor(stp * Math.pow(10, ex[para]));//0:100ms->ms; 1:1s->1000ms
        return sdf.format(new Date(milliSec));
    }
    public static final int hhmmss = 0;
    public static void main(String args[]){
        long StartTime = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("pcaptest").setMaster("local[12]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaPairRDD<IntWritable, BytesWritable> javaRDD =sc.sequenceFile("/mnt/hgfs/LumenShare/file.seq", IntWritable.class, BytesWritable.class);
        JavaPairRDD<IntWritable, BytesWritable> javaRDD =sc.sequenceFile(args[0], IntWritable.class, BytesWritable.class);
        //使用cache会使数据包数据出错
        //javaRDD.cache();

        //生成packet的rdd
        JavaRDD<Packet> packetJavaRDD = javaRDD.map(tt-> (new returnPacket(tt._2)).createPacket());
        //packetJavaRDD.foreach( tt -> System.out.println(tt));

        //筛去非tcp的数据
        JavaRDD<Packet> tcpJavaRDD = packetJavaRDD.filter(Packet::isTCP);
        //tcpJavaRDD.foreach( tt -> System.out.println(tt));
        int outputPara = Integer.parseInt(args[2]);

        int[] ex = {1, 0};
        //生成二元组<时间戳,六元组Rtm（已转为String）>
        JavaPairRDD<Double,String> GroupStrRDD = tcpJavaRDD.mapToPair(new PairFunction<Packet, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Packet packet) throws Exception {
                String str1 = packet.get("ts").toString();
                String str2 = packet.get("ts_micros").toString();
                double ts = Double.parseDouble(str1);
                double micros = Double.parseDouble(str2)/Math.pow(10,6);
                double stp = micros+ts;
                Double timestp = stp*(Math.pow(10,ex[outputPara]));
                Double roughT = Math.floor(timestp);
                return new Tuple2<>(roughT,packet.getRtm().toString());
            }
        });

        //使用combineByKey合并相同key
        //同时将String转为hashmap，<六元组内容,个数>
        JavaPairRDD<Double, HashMap<String,Integer>> EGStrRDD = GroupStrRDD.combineByKey(
                new Function<String, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> call(String s) throws Exception {
                        HashMap<String, Integer> HM = new HashMap<String, Integer>();
                        HM.put(s, 1);
                        return HM;
                    }
                }, new Function2<HashMap<String, Integer>, String, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> call(HashMap<String, Integer> stringIntegerHashMap, String s) throws Exception {
                        if (stringIntegerHashMap.containsKey(s)) {
                            Integer num = stringIntegerHashMap.get(s);
                            stringIntegerHashMap.put(s, ++num);
                        } else {
                            stringIntegerHashMap.put(s, 1);
                        }
                        return stringIntegerHashMap;
                    }
                }, new Function2<HashMap<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> call(HashMap<String, Integer> stringIntegerHashMap, HashMap<String, Integer> stringIntegerHashMap2) throws Exception {
                        if(stringIntegerHashMap.size() < stringIntegerHashMap2.size()){
                            stringIntegerHashMap.forEach((key,value) -> stringIntegerHashMap2.merge(key, value, Integer::sum));
                            stringIntegerHashMap.clear();
                            return stringIntegerHashMap2;
                        } else{
                            stringIntegerHashMap2.forEach((key,value) -> stringIntegerHashMap.merge(key, value, Integer::sum));
                            stringIntegerHashMap2.clear();
                            return stringIntegerHashMap;
                        }
                    }
                }
        );

        //最后是输出
        //方式1 命令行输出
//       以下代码实现命令行输出
//        EGStrRDD.foreach( tt -> {
//            System.out.println("Group"+tt._1+"ms");
//            int rtmnum=0;
//            int num=0;
//            for(Map.Entry<String,Integer> entry: tt._2.entrySet()) {
//                if( entry.getValue()>1 ) rtmnum+=entry.getValue();
//                if( entry.getValue()> Integer.parseInt(args[3])) System.out.println(entry.getKey()+"\t"+entry.getValue());
//                num+=entry.getValue();
//            }
//            System.out.println("rtmnum:" + rtmnum);
//            System.out.println("num:" + num);
//            DecimalFormat df = new DecimalFormat("0.00");
//            String RetransmissionRate = df.format((float) 100*rtmnum / num);
//            System.out.println("RetransmissionRate:" + RetransmissionRate + "%");
//            System.out.println("");
//        });

        //方式2 excel输出
        //以下代码实现excel输出
        //1.将JavaPairRDD<Double, HashMap<String,Integer>>转为JavaPairRDD<Double, String>类型，其中后者的String是重传率（eg. “78.98%”）
        //没有想到更好的转换类型的方法，故使用combineByKey的第一部分功能，但不知是否影响性能
        //使用collect收集到resultList中
        List<Tuple2<Double, String>> resultList =  EGStrRDD.combineByKey(
                new Function<HashMap<String, Integer>, String>() {
                    @Override
                    public String call(HashMap<String, Integer> stringIntegerHashMap) throws Exception {
                        int rtmnum=0;
                        int num=0;
                        for(Map.Entry<String,Integer> entry: stringIntegerHashMap.entrySet()) {
                            if( entry.getValue()>1 ) rtmnum+=entry.getValue();
                            num+=entry.getValue();
                        }
                        DecimalFormat df = new DecimalFormat("0.00");
                        String RetransmissionRate = df.format((float) 100*rtmnum / num) + "%";
                        return RetransmissionRate;
                    }
                }, new Function2<String, HashMap<String, Integer>, String>() {
                    @Override
                    public String call(String s, HashMap<String, Integer> stringIntegerHashMap) throws Exception {
                        return null;
                    }
                }, new Function2<String, String, String>() {
                    @Override
                    public String call(String s, String s2) throws Exception {
                        return null;
                    }
                }
        ).sortByKey().collect();
        //System.out.println(resultList);

        //2.沿用ysh的数据输出方式
        String beginT = stp2time(resultList.get(1)._1, outputPara);
        long startTime3 = System.currentTimeMillis();    //获取开始时间
        int[] bound = {10, 60};
        String fpdata = args[1];
        //String fpG = args[2];
        List<String> outDataList = new ArrayList<>();
        //List<String> outGraphList = new ArrayList<>();
        StringBuilder sbD = new StringBuilder();//for data chart
        //StringBuilder sbG = new StringBuilder();//for chart that generate graph
        switch (outputPara) {//first line
            case 1:
                sbD.append(",0-1s");
                //sbG.append(",time_s");
                for (int i = 1; i < bound[outputPara]; i++) {
                    sbD.append(",");
                    sbD.append(i).append("-").append(i + 1).append("s");
                }
                break;
            case 0:
            default:
                sbD.append(",0-100");
                //sbG.append(",time_100ms");
                for (int i = 1; i < bound[outputPara % 2]; i++) {
                    sbD.append(",");
                    sbD.append(i).append("00-").append(i + 1).append("00");
                }
        }
        String horAxis = sbD.toString();//" ,0-100,100-200,200-300,300-400,400-500,500-600,600-700,700-800,800-900,900-1000";
        outDataList.add(horAxis);
        //outGraphList.add(sbG.toString());
        int p = bound[outputPara % 2];
        int count = 0;
        int isEnd = 0;//isEnd count tt in ResultList,comparing with size of it to tell if we get to the end
        //String timelast = beginT;
        double stpcount = resultList.get(0)._1;
        //1)initialize stpcount with 0 in 100ms or 00 in s
        if(0==outputPara){
            stpcount -= stpcount % 10;
        }else{
            String hms = stp2time(stpcount*10,hhmmss);
            int sec = Integer.parseInt(hms.split(":")[2]);
            stpcount -= sec;
        }
        sbD = new StringBuilder();
        sbD.append(beginT);
        for(Tuple2<Double, String> tt : resultList){
            while(tt._1 != stpcount){
                if(0 == count%p&&count!=0) {
                    outDataList.add(sbD.toString());
                    sbD = new StringBuilder();
                    sbD.append(stp2time(stpcount, outputPara));
                }
                sbD.append(",0");
                stpcount++;
                count++;
            }
            if(0 == count%p&&count!=0) {
                outDataList.add(sbD.toString());
                sbD = new StringBuilder();
                sbD.append(stp2time(stpcount, outputPara));
            }
            sbD.append(",").append(tt._2);
            stpcount++;
            count++;
            isEnd++;
            if (resultList.size() == isEnd && count%p != 0){//add more 0 into the outDataList
                count = count%p;
                for (; p > count; count++) {
                    sbD.append(",0");
                }
                outDataList.add(sbD.toString());
            }
        }

        boolean isSuccess= CSVUtils.exportCsv(new File(fpdata), outDataList);
        System.out.println("Data chart:"+isSuccess+"\n");

        //test speed: get end time
        long endTime3 = System.currentTimeMillis();    //获取结束时间
        System.out.println("write csv time：" + (endTime3 - startTime3) + "ms");
        System.out.println("程序运行时间：" + (endTime3 - StartTime) + "ms");
    }
}
