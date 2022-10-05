import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import scala.Tuple2;

import static scala.math.BigDecimal.binary;

//import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
public class pcaptest2 {
    public static void main(String args[]) throws IOException {
        //初始时间
        long startTime = System.currentTimeMillis();
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.driver.maxResultSize",0)
                .enableHiveSupport()
                .getOrCreate();

        //.config("spark.sql.warehouse.dir","/home/bjbhaha/IdeaProjects/pcap_scala2/pcaptest")
        //spark.sql("CREATE TABLE IF NOT EXISTS src (TIMESTAMP long, TIMESTAMP_USEC long,TIMESTAMP_MICROS long) USING hive OPTIONS(fileFormat 'org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat',outputFormat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',serde 'MySerDe')");
        //spark.sql("CREATE TABLE IF NOT EXISTS src (ts bigint,ts_micros bigint,ttl int,ip_version int,ip_header_length int) USING hive OPTIONS(fileFormat 'sequencefile',serde 'PcapDeserializer')");
        File file = new File("./derby.log"); //相对路径;
        boolean flag=false;
        if(file.exists())
            flag=true;
        spark.sql("CREATE TABLE IF NOT EXISTS src (ts bigint, ts_usec double, protocol string, src string, src_port int, dst string, dst_port int, len int, ttl int, dns_queryid int, dns_flags string, dns_opcode string, dns_rcode string, dns_question string, dns_answer array<string>, dns_authority array<string>, dns_additional array<string>,pcapByte binary) USING hive OPTIONS(fileFormat 'sequencefile',serde 'PcapDeserializer')");
        if(!flag)
            spark.sql("LOAD DATA LOCAL INPATH"+args[0]+" INTO TABLE src");

// Queries are expressed in HiveQL
        //spark.sql("SELECT * FROM src where src regexp '10.222.181.*' and dst regexp '120.240.50.*'").show();
        JavaRDD<Object> pcapByte= spark.sql(args[2]).toJavaRDD().map(row->row.get(0));
        pcapByte.cache();
        //pcapByte.foreach(x->System.out.println(new BytesWritable((byte[])((byte[])(x)))));
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(args[1]));
        byte pcapHeader[] = new byte[]{(byte) 0xD4, (byte) 0xC3, (byte) 0xB2, (byte) 0xA1, 0x02, 0x00, 0x04,
                0x00,0x00, 0x00, 0x00, 0x00, 0x00, 0x00,0x00,0x00,0x00,0x00,0x04,0x00,0x01,0x00,0x00,0x00};
        dos.write(pcapHeader,0,24);
        List<byte[]> list=new ArrayList<>();
        pcapByte.foreach(x->{
            int a=0;
        });
        long endTime = System.currentTimeMillis();
        long startTime1 = System.currentTimeMillis();
        pcapByte.collect().forEach(tt->{
            byte[] a=(byte[])((byte[])(tt));
            long packetSize = PcapReaderUtil.convertInt(a, 8, false);
            int l=(int)packetSize+16;
            try {
                dos.write(a, 0, l);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        System.out.println("search运行时间：" + (endTime - startTime) + "ms");
        long endTime1 = System.currentTimeMillis();
        System.out.println("merge运行时间：" + (endTime1 - startTime1) + "ms");
        //spark.sql("SELECT * FROM src").show();
    }
}
