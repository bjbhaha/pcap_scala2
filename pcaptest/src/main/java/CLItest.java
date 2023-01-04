import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Scanner;
public class CLItest {
    public static void OptionPrint() {
        System.out.println("--------------------MENU------------------------");
        System.out.println("1. Flow statistics");
        System.out.println("2. Retransmission rate");
        System.out.println("3. Timestamp filter!");
        System.out.println("4. IP filter");
        System.out.println("5. Tcp flow aggregation");
        System.out.println("6. Custom Query");
        System.out.println("7. Combination of search and calculate");//!!
        System.out.println("0. Quit!");
        System.out.println("please input which item you want to calculate");
        System.out.println("-----------------------------------------------");
         /*
        5 and 1,2,4
        4 and 1,2
        3 and 1,2,4,5
        */
        //3 4 5
        //3 4 5|| 1 2 ||
    }

    public static void flowCaculate() {
        System.out.println("flowCaculate: please input ...h");
        System.out.println("usage:* 5 parameters in para list\n" +
                "* [path of input seqfile] [path of output data chart] [path of output graph source chart] [bps or pps] [choice]\n" +
                "* choice:0-100ms, 1-1s\n" +
                "* example:\n" +
                "* /home/ysh/IdeaProjects/pcap_scala3/repo/bigfile.seq\n" +
                "/home/ysh/IdeaProjects/pcap_scala3/repo/testmsD100bps.csv\n" +
                "/home/ysh/IdeaProjects/pcap_scala3/repo/testmsG100bps.csv\n" +
                "bps\n" +
                "0\n" +
                "* ");
        Scanner input = new Scanner(System.in);
        String s = input.next();
        statis2.main(s.split(","));
    }

    public static void Retransmission() {
        System.out.println("Retransmission: please input ...h");
        System.out.println("usage:....");
        Scanner input = new Scanner(System.in);
        String s = input.next();
        retran.main(s.split(","));
    }
    public static void SQLquery() throws IOException {
        System.out.println("SQLquery: please input which sql you want to search");
        System.out.println("usage:\n example:\n * \"'hdfs:///user/hive/warehouse/src/line12.seq'\" \n" +
                "        * \"/home/baijb/guopai/file_cluster.pcap\" \n" +
                "        * \"SELECT pcapByte FROM src where src regexp '58.37.33.71'\" \n" +
                "        * 1");
//        Scanner input = new Scanner(System.in);
//        String s1 = input.next();
//        System.out.println("please input sequence File");
//        String s2 = input.next();
//        System.out.println("please input the output File");
//        String s3 = input.next();
//        System.out.println("please input the sql");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        pcaptest2.main(s.split(","));
    }

    public static void timestampFilter() throws IOException {
        System.out.println("timestampFilter: please input start time and end time");
        System.out.println("usage:....");
        //Scanner input = new Scanner(System.in);
        //String s = input.next();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
//        pcaptest2.main(s.split(","));
        //ShellUtils.exceShell('./spark-submit   --class pcaptest2      --master yarn   --deploy-mode cluster   --executor-memory 20G  --driver-memory 10G  --num-executors 2 --executor-cores 60  /home/baijb/pcap_scala2_jar/pcap_scala2.jar \\ "\'hdfs:///user/hive/warehouse/src/line12.seq\'" "/home/baijb/guopai/file_cluster.pcap" "SELECT pcapByte FROM src where src regexp \'58.37.33.71\'" 1');
        //ShellUtils.exceShell("/home/baijb/spark/Spark24/bin/spark-submit   --class pcaptest2      --master yarn   --deploy-mode cluster   --executor-memory 20G  --driver-memory 10G  --num-executors 2 --executor-cores 60  /home/baijb/pcap_scala2_jar/pcap_scala2.jar \\ \"'hdfs:///user/hive/warehouse/src/line12.seq'\" \"/home/baijb/guopai/file_cluster.pcap\" \"SELECT pcapByte FROM src where src regexp '58.37.33.71'\" 1");
        //Process process = Runtime.getRuntime().exec("/home/baijb/spark/Spark24/bin/spark-submit   --class pcaptest2      --master yarn   --deploy-mode cluster   --executor-memory 20G  --driver-memory 10G  --num-executors 2 --executor-cores 60  /home/baijb/pcap_scala2_jar/pcap_scala2.jar \\ \"'hdfs:///user/hive/warehouse/src/line12.seq'\" \"/home/baijb/guopai/file_cluster.pcap\" \"SELECT pcapByte FROM src where src regexp '58.37.33.71'\" 1");
        String args[]=s.split(",");
        long st=Date.parse(args[0]);//ts_micros
        long tt=Date.parse(args[1]);
        System.out.println("start:"+st);
        System.out.println("end:"+tt);
        pcaptest2.main(new String[]{"'hdfs:///user/hive/warehouse/src/file3.seq'","/home/baijb/guoPai/file_cluster.pcap","SELECT pcapByte FROM src where ts > "+st+" and ts < "+tt,"1"});
    }

    public static void flowAggregate() throws IOException {
        System.out.println("flowAggregate: please input srcIp dstIp srcPort dstPort");
        System.out.println("usage:....");
//        Scanner input = new Scanner(System.in);
//        String s = input.nextLine();
//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//        String s = br.readLine();
//        pcaptest2.main(s.split(","));
        Scanner input = new Scanner(System.in);
        String s = input.next();
        String args[]=s.split(",");
        pcaptest2.main(new String[]{"'hdfs:///user/hive/warehouse/src/file3.seq'","/home/baijb/guoPai/file_cluster.pcap",
                "SELECT pcapByte FROM src where src regexp '"+args[0]+"' and dst regexp '"+args[1]+"' and src_port = "+args[2]
                        +" and dst_port = "+args[3]+" or src regexp '"+args[1]+"' and dst regexp '"+args[0]+"' and src_port = "+args[3]
                        +" and dst_port = "+args[2],"1"});
    }

    public static void ipFilter() throws IOException {
        System.out.println("ipFilter: please input srcIp or dstIp ");
        System.out.println("usage:....");
        Scanner input = new Scanner(System.in);

//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//        String s = br.readLine();
//        pcaptest2.main(s.split(","));
        //'hdfs:///user/hive/warehouse/src/file3.seq',/home/baijb/guoPai/file_cluster.pcap,SELECT pcapByte FROM src where src regexp '58.37.33.71' or dst regexp '58.37.33.71',1
        String s = input.next();
        pcaptest2.main(new String[]{"'hdfs:///user/hive/warehouse/src/file3.seq'","/home/baijb/guoPai/file_cluster.pcap","SELECT pcapByte FROM src where src regexp '"+s+"' or dst regexp '"+s+"'","1"});
    }
    public static void combineSaC() throws IOException {

        System.out.println("combineSaC: please input ...h");
        System.out.println("usage:....");
//        Scanner input = new Scanner(System.in);
//        String s = input.nextLine();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        //pcaptest2.main(new String[]{"'/home/bjbhaha/Envroment/hadoop-2.7.3/bin/file.seq'", "/home/bjbhaha/Desktop/file.pcap", "SELECT pcapByte FROM src where src regexp '121.207.227.12' and dst regexp '192.168.31.34'", "1"});
        pcaptest2.main(s.split(","));
    }
    public static void main(String args[]) throws IOException {
        boolean flag = true;
        int option;
        Scanner input = new Scanner(System.in);
        while (flag) {
            OptionPrint();
            option = input.nextInt();
            switch (option) {
                case 1:
                    flowCaculate();
                    break;
                case 2:
                    Retransmission();
                    break;
                case 3:
                    timestampFilter();
                    break;
                case 4:
                    ipFilter();
                    break;
                case 5:
                    flowAggregate();
                    break;
                case 6:
                    SQLquery();
                    break;
                case 7:
                    combineSaC();
                    break;
                case 0:
                    flag=false;
                    break;
            }
        }
    }
}
