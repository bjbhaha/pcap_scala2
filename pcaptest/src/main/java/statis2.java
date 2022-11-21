/*
* 5 parameters in para list
* [path of input seqfile] [path of output data chart] [path of output graph source chart] [bps or pps] [choice]
* choice:0-100ms, 1-1s
* */


import au.com.bytecode.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class statis2 {
    public static final int hhmmss = 0;//stp->time:"HH:MM:SS"

    public static String stp2time(double stp, int para) {
        String[] format = {"HH:mm:ss", "HH:mm"};
        int[] ex = {2, 3};
        SimpleDateFormat sdf = new SimpleDateFormat(format[para]);
        long milliSec = (long) Math.floor(stp * Math.pow(10, ex[para]));//0:100ms->ms; 1:1s->1000ms
        return sdf.format(new Date(milliSec));
    }

    public static void main(String[] args) {
        //test the speed
        long startTime1 = System.currentTimeMillis();    //获取开始时间

        //spark context
        SparkConf conf = new SparkConf().setAppName("Statis_ts").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<IntWritable, BytesWritable> javaRDD = sc.sequenceFile(args[0], IntWritable.class, BytesWritable.class);

        int outputPara = Integer.parseInt(args[4]);
        String unitPara = (String)(args[3]);
        boolean f = false;
        if(unitPara.equals("pps"))
            f = true;
        //ysh
        // 1. maptoPair to get new pairRDD<timestp,size>
        int[] ex = {1, 0};
        boolean finalF = f;
        JavaPairRDD<Double, Long> rtsRDD = javaRDD.mapToPair(new PairFunction<Tuple2<IntWritable, BytesWritable>, Double, Long>() {
            public Tuple2<Double, Long> call(Tuple2<IntWritable, BytesWritable> tt) throws Exception {
                Packet packet = (new returnPacket(tt._2)).createPacket();
                //combine ts_usec and ts_micros
                String str1 = packet.get("ts").toString();
                String str2 = packet.get("ts_micros").toString();
                double ts = Double.parseDouble(str1);
                double micros = Double.parseDouble(str2) / Math.pow(10, 9);
                double stp = micros + ts;//μs 10+9
                double timestp = stp * (Math.pow(10, ex[outputPara]));//"0"-100ms,"1"-1s
                Double roughT = Math.floor(timestp);//roughT=[timestp]
                Long size = (long) tt._2.getLength();//Byte for bps or bp100ms
                if(finalF)
                    size = Long.valueOf(1);//for pps
                return new Tuple2<>(roughT, size);
            }
        });
        rtsRDD.cache();


        //2.reduceByKey&sortByKey
        JavaPairRDD<Double, Long> rtsRDD1 = rtsRDD.reduceByKey((a, b) -> a + b);//interval accumu
        JavaPairRDD<Double, Long> rtsRDDsorted = rtsRDD1.sortByKey(true);

        long endTime1 = System.currentTimeMillis();    //获取结束时间
        System.out.println("statisTime：" + (endTime1 - startTime1) + "ms");

        //3.test to output to terminal
        //1)get beginning time from 100ms instead of millisec
        //2)get records
        long startTime2 = System.currentTimeMillis();    //获取开始时间

        List<Tuple2<Double, Long>> resultList = new ArrayList<>();
        resultList = rtsRDDsorted.collect();//problem:
        String beginT = stp2time(resultList.get(0)._1, outputPara);

        long endTime2 = System.currentTimeMillis();    //获取结束时间
        System.out.println("collectTime：" + (endTime2 - startTime2) + "ms");

        //boolean flag = false;
        /*for(Tuple2<Double,Long>tt:resultList){
            String str = new String();
            str = String.format("%.0f",(double)(tt._1));
            System.out.println("time is "+stp2time((double)tt._1,0)+" interval start at "+str+" size is "+tt._2+"\n");
        }
        System.out.println("resultList length is "+resultList.size());*/

        //4.write to csv
        //dynamically change vertical axis
        long startTime3 = System.currentTimeMillis();    //获取开始时间
        int[] bound = {10, 60};
        String fpdata = args[1];
        String fpG = args[2];
        List<String> outDataList = new ArrayList<>();
        List<String> outGraphList = new ArrayList<>();
        StringBuilder sbD = new StringBuilder();//for data chart
        StringBuilder sbG = new StringBuilder();//for chart that generate graph
        switch (outputPara) {//first line
            case 1:
                sbD.append(",0-1s");
                sbG.append(",time_s");
                for (int i = 1; i < bound[outputPara]; i++) {
                    sbD.append(",");
                    sbD.append(i).append("-").append(i + 1).append("s");
                }
                break;
            case 0:
            default:
                sbD.append(",0-100");
                sbG.append(",time_100ms");
                for (int i = 1; i < bound[outputPara % 2]; i++) {
                    sbD.append(",");
                    sbD.append(i).append("00-").append(i + 1).append("00");
                }
        }
        String horAxis = sbD.toString();//" ,0-100,100-200,200-300,300-400,400-500,500-600,600-700,700-800,800-900,900-1000";
        outDataList.add(horAxis);
        outGraphList.add(sbG.toString());
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
        sbG = new StringBuilder();
        sbD.append(beginT);
        for(Tuple2<Double, Long> tt : resultList){
            while(tt._1 != stpcount){
                if(0 == outputPara)
                    sbG.append(stp2time(stpcount, outputPara)).append(".").append(count%p);
                else
                    sbG.append(stp2time(stpcount*10,hhmmss));//convert stpcount from s to 100ms
                sbG.append(",0");
                outGraphList.add(sbG.toString());
                sbG = new StringBuilder();
                if(0 == count%p && count!=0) {//change the line
                    outDataList.add(sbD.toString());
                    sbD = new StringBuilder();
                    sbD.append(stp2time(stpcount, outputPara));
                }
                sbD.append(",0");

                stpcount++;
                count++;
            }
            if(0 == outputPara)
                sbG.append(stp2time(stpcount, outputPara)).append(".").append(count%p);
            else
                sbG.append(stp2time(stpcount*10,hhmmss));
            sbG.append(",").append(tt._2);
            outGraphList.add(sbG.toString());
            sbG = new StringBuilder();
            if(0 == count%p && count!=0) {
                outDataList.add(sbD.toString());
                sbD = new StringBuilder();
                sbD.append(stp2time(stpcount, outputPara));
            }
            sbD.append(",").append(tt._2);

            stpcount++;
            count++;

            isEnd++;
            if (resultList.size() == isEnd ){//&& count%p != 0){//add more 0 into the outDataList
                count = count%p;
                if(count != 0){
                    for (; p > count; count++) {
                        sbD.append(",0");
                    }
                }
                outDataList.add(sbD.toString());
            }
        }

        boolean isSuccess= CSVUtils.exportCsv(new File(fpdata), outDataList);
        boolean isSuccess1= CSVUtils.exportCsv(new File(fpG), outGraphList);
        System.out.println("Data chart:"+isSuccess+"\n");
        System.out.println("Graph data source chart:"+isSuccess1+"\n");


        //test speed: get end time
        long endTime3 = System.currentTimeMillis();    //获取结束时间
        System.out.println("write csv time：" + (endTime3 - startTime3) + "ms");
        System.out.println("程序运行时间：" + (endTime3 - startTime1) + "ms");
    }

}
